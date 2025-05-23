import { retryOperation } from './retry.js';

/**
 * Сервис для CRUD операций с Google Sheets
 * ИСПРАВЛЕНО: правильное использование valueInputOption для дат
 */

export interface SheetRange {
	start: number;
	end: number;
}

export interface SheetServiceConfig {
	sheets: any;
	sheetId: string;
	sheetName: string;
}

export class SheetService {
	private sheets: any;
	private sheetId: string;
	private sheetName: string;

	constructor(config: SheetServiceConfig) {
		this.sheets = config.sheets;
		this.sheetId = config.sheetId;
		this.sheetName = config.sheetName;
	}

	/**
	 * Получает все данные с листа (без ограничения в 10000 строк)
	 */
	async getAllData(): Promise<string[][]> {
		return await retryOperation(async () => {
			// Сначала получаем размеры листа
			const sheetInfo = await this.sheets.spreadsheets.get({
				spreadsheetId: this.sheetId,
				ranges: [this.sheetName],
				fields: 'sheets.properties'
			});

			const sheet = sheetInfo.data.sheets?.[0];
			const gridProperties = sheet?.properties?.gridProperties;

			if (!gridProperties) {
				return [];
			}

			const actualRowCount = gridProperties.rowCount || 0;
			const actualColumnCount = gridProperties.columnCount || 0;

			if (actualRowCount === 0) {
				return [];
			}

			// Определяем реальный диапазон данных
			const lastColumn = this.numberToColumnLetter(actualColumnCount);
			console.log(`📏 Sheet dimensions: ${actualRowCount} rows × ${actualColumnCount} columns`);

			// Читаем все данные в пределах реальных размеров листа
			const dataResponse = await this.sheets.spreadsheets.values.get({
				spreadsheetId: this.sheetId,
				range: `${this.sheetName}!A1:${lastColumn}${actualRowCount}`,
				// ИСПРАВЛЕНО: используем FORMATTED_VALUE для получения отформатированных дат
				valueRenderOption: 'FORMATTED_VALUE'
			});

			return dataResponse.data.values || [];
		});
	}

	/**
	 * Очищает значения в указанных строках (сохраняя структуру)
	 */
	async clearRowValues(rowRanges: SheetRange[], columnCount: number): Promise<void> {
		if (rowRanges.length === 0) return;

		const lastCol = this.numberToColumnLetter(columnCount);

		for (const range of rowRanges) {
			const rangeStr = range.start === range.end
				? `${this.sheetName}!A${range.start}:${lastCol}${range.start}`
				: `${this.sheetName}!A${range.start}:${lastCol}${range.end}`;

			await retryOperation(async () => {
				await this.sheets.spreadsheets.values.clear({
					spreadsheetId: this.sheetId,
					range: rangeStr
				});
			});

			// Задержка между операциями
			await this.delay(200);
		}
	}

	/**
	 * БЕЗОПАСНО удаляет строки (по одному диапазону за раз, сверху вниз)
	 */
	async deleteRows(rowRanges: SheetRange[]): Promise<void> {
		if (rowRanges.length === 0) return;

		const sheetId = await this.getSheetId();

		// Сортируем диапазоны по убыванию (удаляем сверху вниз)
		const sortedRanges = [...rowRanges].sort((a, b) => b.start - a.start);

		console.log(`🗑️  Deleting ${sortedRanges.length} row ranges...`);

		// Удаляем по одному диапазону для избежания смещения индексов
		for (const range of sortedRanges) {
			try {
				await retryOperation(async () => {
					await this.sheets.spreadsheets.batchUpdate({
						spreadsheetId: this.sheetId,
						requestBody: {
							requests: [{
								deleteDimension: {
									range: {
										sheetId: sheetId,
										dimension: 'ROWS',
										startIndex: range.start - 1, // Google Sheets 0-indexed для API
										endIndex: range.end // endIndex не включается
									}
								}
							}]
						}
					});
				});

				console.log(`✅ Deleted rows ${range.start}-${range.end}`);

				// Задержка между удалениями для стабильности
				await this.delay(300);

			} catch (error) {
				console.error(`❌ Failed to delete rows ${range.start}-${range.end}:`, error);
				// Продолжаем удалять остальные диапазоны
			}
		}
	}

	/**
	 * Добавляет новые строки в конец листа
	 * ИСПРАВЛЕНО: использует USER_ENTERED по умолчанию для правильного форматирования дат
	 */
	async appendRows(data: any[][], valueInputOption: 'RAW' | 'USER_ENTERED' = 'USER_ENTERED'): Promise<void> {
		await retryOperation(async () => {
			await this.sheets.spreadsheets.values.append({
				spreadsheetId: this.sheetId,
				range: this.sheetName,
				valueInputOption,
				insertDataOption: 'INSERT_ROWS',
				requestBody: { values: data }
			});
		});
	}

	/**
	 * Полная перезапись листа
	 * ИСПРАВЛЕНО: использует USER_ENTERED для правильного форматирования дат
	 */
	async replaceAllData(data: any[][], columnCount: number): Promise<void> {
		const columnRange = `A:${this.numberToColumnLetter(columnCount)}`;

		await retryOperation(async () => {
			// Очищаем только значения, сохраняя форматирование
			await this.sheets.spreadsheets.values.clear({
				spreadsheetId: this.sheetId,
				range: `${this.sheetName}!${columnRange}`
			});

			// Записываем новые данные с USER_ENTERED для автоматического форматирования
			await this.sheets.spreadsheets.values.update({
				spreadsheetId: this.sheetId,
				range: `${this.sheetName}!A1`,
				valueInputOption: 'USER_ENTERED', // ИСПРАВЛЕНО: было 'RAW'
				requestBody: { values: data }
			});
		});
	}

	/**
	 * Добавляет новые колонки в заголовки
	 */
	async addColumns(newColumns: string[], startColumn: number): Promise<void> {
		if (newColumns.length === 0) return;

		const startColumnLetter = this.numberToColumnLetter(startColumn + 1);

		await retryOperation(async () => {
			await this.sheets.spreadsheets.values.update({
				spreadsheetId: this.sheetId,
				range: `${this.sheetName}!${startColumnLetter}1`,
				valueInputOption: 'RAW', // Заголовки всегда как RAW
				requestBody: { values: [newColumns] }
			});
		});
	}

	/**
	 * Форматирует колонки с датами
	 */
	async formatDateColumns(dateColumnIndices: number[]): Promise<void> {
		if (dateColumnIndices.length === 0) return;

		try {
			const sheetId = await this.getSheetId();

			const requests = dateColumnIndices.map(columnIndex => ({
				repeatCell: {
					range: {
						sheetId: sheetId,
						startColumnIndex: columnIndex,
						endColumnIndex: columnIndex + 1
					},
					cell: {
						userEnteredFormat: {
							numberFormat: {
								type: 'DATE',
								pattern: 'yyyy-mm-dd'
							}
						}
					},
					fields: 'userEnteredFormat.numberFormat'
				}
			}));

			await retryOperation(async () => {
				await this.sheets.spreadsheets.batchUpdate({
					spreadsheetId: this.sheetId,
					requestBody: { requests }
				});
			});
		} catch (error) {
			console.warn('⚠️  Could not format date columns:', error);
			// Не критично, продолжаем работу
		}
	}

	/**
	 * Получает количество доступных строк в листе
	 */
	async getAvailableRows(): Promise<number> {
		try {
			const response = await retryOperation(async () => {
				return await this.sheets.spreadsheets.get({
					spreadsheetId: this.sheetId,
					ranges: [this.sheetName],
					fields: 'sheets.properties'
				});
			});

			const sheet = response.data.sheets?.[0];
			if (sheet && sheet.properties && sheet.properties.gridProperties) {
				return sheet.properties.gridProperties.rowCount || 0;
			}

			return 0;
		} catch (error) {
			console.warn('⚠️  Could not get sheet row count:', error);
			return 0;
		}
	}

	/**
	 * Добавляет строки к листу
	 */
	async addRowsToSheet(rowsToAdd: number): Promise<void> {
		try {
			const sheetId = await this.getSheetId();

			await retryOperation(async () => {
				return await this.sheets.spreadsheets.batchUpdate({
					spreadsheetId: this.sheetId,
					requestBody: {
						requests: [{
							appendDimension: {
								sheetId: sheetId,
								dimension: 'ROWS',
								length: rowsToAdd
							}
						}]
					}
				});
			});

			console.log(`✅ Added ${rowsToAdd} rows to sheet`);
		} catch (error) {
			console.warn('⚠️  Could not add rows to sheet:', error);
			// Не критично, append может сработать и без предварительного добавления строк
		}
	}

	/**
	 * Записывает простое сообщение в A1
	 */
	async writeMessage(message: string): Promise<void> {
		await retryOperation(async () => {
			await this.sheets.spreadsheets.values.update({
				spreadsheetId: this.sheetId,
				range: `${this.sheetName}!A1`,
				valueInputOption: 'RAW',
				requestBody: { values: [[message]] }
			});
		});
	}

	/**
	 * Получает ID листа по имени
	 */
	private async getSheetId(): Promise<number> {
		try {
			const response = await retryOperation(async () => {
				return await this.sheets.spreadsheets.get({
					spreadsheetId: this.sheetId,
					fields: 'sheets.properties'
				});
			});

			const sheet = response.data.sheets?.find((s: any) => s.properties?.title === this.sheetName);
			return sheet?.properties?.sheetId || 0;
		} catch (error) {
			console.warn('⚠️  Could not get sheet ID:', error);
			return 0;
		}
	}

	/**
	 * Преобразует номер колонки в букву (1 = A, 26 = Z, 27 = AA, etc.)
	 */
	private numberToColumnLetter(columnNumber: number): string {
		let result = '';
		while (columnNumber > 0) {
			columnNumber--; // Делаем 0-based
			result = String.fromCharCode(65 + (columnNumber % 26)) + result;
			columnNumber = Math.floor(columnNumber / 26);
		}
		return result;
	}

	/**
	 * Задержка
	 */
	private async delay(ms: number): Promise<void> {
		return new Promise(resolve => setTimeout(resolve, ms));
	}
}
