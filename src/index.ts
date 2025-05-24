import { google } from 'googleapis';
import fs from 'fs/promises';
import { SheetService } from '../sheets/service.js';
import { RecordMapper } from '../sheets/mapper.js';

/**
 * Идеальный Google Sheets writer
 * ИСПРАВЛЕНО: правильная обработка дат и valueInputOption
 */

interface WriteStats {
	totalRecords: number;
	newRecords: number;
	updatedRecords: number;
	deletedRecords: number;
	newColumns: string[];
}

export class GoogleSheetsWriter {
	private sheetService: SheetService;
	private mapper: RecordMapper;
	private fullRefresh: boolean;
	private dryRun: boolean;
	private lockFile: string;

	constructor() {
		// Получаем конфигурацию из переменных окружения
		const sheetId = process.env.GOOGLE_SHEET_ID!;
		const sheetName = process.env.SHEET_NAME!;
		this.fullRefresh = process.env.FULL_REFRESH === 'true';
		this.dryRun = process.env.DRY_RUN === 'true';
		this.lockFile = `.writer-lock-${sheetId}-${sheetName}`;

		if (!sheetId || !sheetName || !process.env.GOOGLE_SERVICE_ACCOUNT_JSON) {
			throw new Error('Missing required environment variables: GOOGLE_SHEET_ID, SHEET_NAME, GOOGLE_SERVICE_ACCOUNT_JSON');
		}

		// Инициализация Google Sheets API
		const creds = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON);
		const auth = new google.auth.JWT(
			creds.client_email,
			undefined,
			creds.private_key,
			['https://www.googleapis.com/auth/spreadsheets']
		);
		const sheets = google.sheets({ version: 'v4', auth });

		// Инициализация сервисов
		this.sheetService = new SheetService({ sheets, sheetId, sheetName });
		this.mapper = new RecordMapper(); // Теперь поддерживает кастомные поля дат
	}

	/**
	 * Основной метод записи данных с защитой от параллельных запусков
	 */
	async writeData(dataPath: string): Promise<WriteStats> {
		console.log(`📊 Starting data write from ${dataPath}${this.dryRun ? ' (DRY RUN)' : ''}`);

		// Проверяем блокировку (только если не dry-run)
		if (!this.dryRun) {
			await this.acquireLock();
		}

		try {
			const newRecords = await this.loadData(dataPath);

			if (newRecords.length === 0) {
				throw new Error('❌ No data to write');
			}

			// if (newRecords.length === 0) {
			// 	console.log('⚠️  No data found');
			//
			// 	if (!this.dryRun) {
			// 		await this.sheetService.writeMessage('No data available');
			// 	}
			//
			// 	return {
			// 		totalRecords: 0,
			// 		newRecords: 0,
			// 		updatedRecords: 0,
			// 		deletedRecords: 0,
			// 		newColumns: []
			// 	};
			// }

			console.log(`📥 Loaded ${newRecords.length} new records`);

			if (this.fullRefresh) {
				return await this.performFullRefresh(newRecords);
			} else {
				return await this.performIncrementalUpdate(newRecords);
			}
		} finally {
			// Всегда освобождаем блокировку
			if (!this.dryRun) {
				await this.releaseLock();
			}
		}
	}

	/**
	 * Создает блокировку для предотвращения параллельных запусков
	 */
	private async acquireLock(): Promise<void> {
		try {
			// Проверяем существование lock файла
			await fs.access(this.lockFile);

			// Файл существует - читаем время создания
			const stats = await fs.stat(this.lockFile);
			const lockAge = Date.now() - stats.mtime.getTime();

			// Если блокировка старше 30 минут - считаем зависшей
			if (lockAge > 30 * 60 * 1000) {
				console.warn('⚠️  Found stale lock file, removing...');
				await fs.unlink(this.lockFile);
			} else {
				throw new Error(`Another writer process is running (lock age: ${Math.round(lockAge / 1000)}s). Please wait or remove ${this.lockFile}`);
			}
		} catch (error: any) {
			if (error.code !== 'ENOENT') {
				throw error;
			}
		}

		// Создаем новую блокировку
		const lockData = {
			pid: process.pid,
			timestamp: new Date().toISOString(),
			sheet: process.env.SHEET_NAME
		};

		await fs.writeFile(this.lockFile, JSON.stringify(lockData, null, 2));
		console.log(`🔒 Acquired lock: ${this.lockFile}`);
	}

	/**
	 * Освобождает блокировку
	 */
	private async releaseLock(): Promise<void> {
		try {
			await fs.unlink(this.lockFile);
			console.log(`🔓 Released lock: ${this.lockFile}`);
		} catch (error) {
			console.warn('⚠️  Could not remove lock file:', error);
		}
	}

	/**
	 * Загружает данные из JSON файла
	 */
	private async loadData(dataPath: string): Promise<Record<string, any>[]> {
		const rawData = await fs.readFile(dataPath, 'utf-8');
		return JSON.parse(rawData) as Record<string, any>[];
	}

	/**
	 * Полное обновление - очистка и запись всех данных
	 * ИСПРАВЛЕНО: используем USER_ENTERED для правильного форматирования дат
	 */
	private async performFullRefresh(newRecords: Record<string, any>[]): Promise<WriteStats> {
		console.log('🔄 Performing full refresh');

		const allColumns = this.mapper.collectAllFields(newRecords);
		console.log(`📋 Columns to write: ${allColumns.join(', ')}`);

		const dataRows = this.mapper.recordsToRows(newRecords, allColumns);
		const allData = [allColumns, ...dataRows];

		if (!this.dryRun) {
			await this.sheetService.replaceAllData(allData, allColumns.length);

			// Форматируем колонки с датами
			const dateColumnIndices = this.mapper.findDateColumnIndices(allColumns);
			if (dateColumnIndices.length > 0) {
				await this.sheetService.formatDateColumns(dateColumnIndices);
			}
		}

		console.log(`✅ Full refresh ${this.dryRun ? 'analyzed' : 'completed'}: ${newRecords.length} records`);

		return {
			totalRecords: newRecords.length,
			newRecords: newRecords.length,
			updatedRecords: 0,
			deletedRecords: 0,
			newColumns: allColumns
		};
	}

	/**
	 * Инкрементальное обновление с реальным удалением строк
	 * ИСПРАВЛЕНО: используем USER_ENTERED для правильного форматирования дат
	 */
	private async performIncrementalUpdate(newRecords: Record<string, any>[]): Promise<WriteStats> {
		console.log('➕ Performing incremental update with row deletion');

		// Загружаем существующие данные
		const existingData = await this.sheetService.getAllData();

		if (existingData.length === 0) {
			console.log('📄 Sheet is empty, performing initial write');
			return await this.performFullRefresh(newRecords);
		}

		const existingHeaders = existingData[0];
		const existingRecords = this.mapper.rowsToRecords(existingData, existingHeaders);

		console.log(`📚 Loaded ${existingRecords.length} existing records with ${existingHeaders.length} columns`);

		// Определяем даты для удаления
		const datesToUpdate = this.mapper.extractDatesFromRecords(newRecords);
		console.log(`📅 Dates to update: ${datesToUpdate.join(', ')}`);

		// Находим строки для удаления
		const rowsToDelete = this.findRowsToDelete(existingRecords, datesToUpdate);
		let deletedCount = 0;

		if (rowsToDelete.length > 0) {
			console.log(`🗑️  Will delete ${rowsToDelete.length} rows for dates: ${datesToUpdate.join(', ')}`);

			if (!this.dryRun) {
				// РЕАЛЬНО удаляем строки (deleteDimension)
				const ranges = this.mapper.groupConsecutiveRanges(rowsToDelete);
				await this.sheetService.deleteRows(ranges);
			}

			deletedCount = rowsToDelete.length;
		}

		// Определяем новые колонки
		const newFields = this.mapper.collectAllFields(newRecords);
		const finalColumns = this.mapper.mergeColumns(existingHeaders, newFields);
		const newColumns = finalColumns.filter((col: string) => !existingHeaders.includes(col));

		// Добавляем новые колонки если нужно
		if (newColumns.length > 0) {
			console.log(`➕ Adding new columns: ${newColumns.join(', ')}`);

			if (!this.dryRun) {
				await this.sheetService.addColumns(newColumns, existingHeaders.length);
			}
		}

		// Проверяем достаточно ли строк в таблице
		const currentDataRows = existingRecords.length - deletedCount;
		const requiredRows = currentDataRows + newRecords.length + 1; // +1 для заголовка

		if (!this.dryRun) {
			const availableRows = await this.sheetService.getAvailableRows();
			console.log(`📏 Current data rows: ${currentDataRows}, Available rows: ${availableRows}, Required: ${requiredRows}`);

			if (requiredRows > availableRows || availableRows === 0) {
				console.log(`📈 Need more rows. Adding 5000 rows to sheet...`);
				await this.sheetService.addRowsToSheet(5000);
			}
		}

		// Добавляем новые данные
		if (newRecords.length > 0) {
			console.log(`📝 ${this.dryRun ? 'Would append' : 'Appending'} ${newRecords.length} new records`);

			if (!this.dryRun) {
				const dataRows = this.mapper.recordsToRows(newRecords, finalColumns);
				// ИСПРАВЛЕНО: USER_ENTERED для автоматического форматирования строк-дат
				await this.sheetService.appendRows(dataRows, 'USER_ENTERED');

				// Форматируем колонки с датами (дополнительная страховка)
				const dateColumnIndices = this.mapper.findDateColumnIndices(finalColumns);
				if (dateColumnIndices.length > 0) {
					await this.sheetService.formatDateColumns(dateColumnIndices);
				}
			}
		}

		const stats: WriteStats = {
			totalRecords: currentDataRows + newRecords.length,
			newRecords: newRecords.length,
			updatedRecords: 0,
			deletedRecords: deletedCount,
			newColumns
		};

		console.log(`✅ Incremental update ${this.dryRun ? 'analyzed' : 'completed'}:`, stats);
		return stats;
	}

	/**
	 * Находит номера строк для удаления (с нормализацией дат)
	 */
	private findRowsToDelete(existingRecords: Array<{data: Record<string, any>, rowNumber?: number}>, datesToUpdate: string[]): number[] {
		const rowsToDelete: number[] = [];

		for (const record of existingRecords) {
			const rawDate = this.mapper.getRecordDate(record.data);
			const normalizedDate = this.mapper.normalizeDate(rawDate);

			if (datesToUpdate.includes(normalizedDate) && record.rowNumber) {
				rowsToDelete.push(record.rowNumber);
			}
		}

		return rowsToDelete.sort((a, b) => b - a); // Удаляем с конца
	}
}

/**
 * CLI интерфейс с поддержкой dry-run
 */
async function main() {
	const args = process.argv.slice(2);
	const dataPath = args[0] || 'data.json';
	const isDryRun = args.includes('--dry-run');

	// Устанавливаем DRY_RUN из аргумента командной строки
	if (isDryRun) {
		process.env.DRY_RUN = 'true';
	}

	try {
		const writer = new GoogleSheetsWriter();
		const stats = await writer.writeData(dataPath);

		console.log('\n📊 Final Statistics:');
		console.log(`   Total records: ${stats.totalRecords}`);
		console.log(`   New records: ${stats.newRecords}`);
		console.log(`   Deleted records: ${stats.deletedRecords}`);

		if (stats.newColumns.length > 0) {
			console.log(`   New columns added: ${stats.newColumns.join(', ')}`);
		}

		if (isDryRun) {
			console.log('\n🔍 DRY RUN completed - no actual changes made to sheet');
		} else {
			console.log('\n🎉 Google Sheets write completed successfully!');
		}

	} catch (error) {
		console.error('💥 Google Sheets Writer error:', error);
		process.exit(1);
	}
}

main();
