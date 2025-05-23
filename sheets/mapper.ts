/**
 * Маппер для преобразования JSON данных в строки Google Sheets
 * Обрабатывает типы данных, даты, вложенные объекты
 */

export interface ProcessedRecord {
	data: Record<string, any>;
	rowNumber?: number;
}

export class RecordMapper {
	private dateFields: string[];

	constructor(customDateFields?: string[]) {
		// Позволяем настраивать поля дат через переменную окружения или конструктор
		const envDateFields = process.env.DATE_FIELDS?.split(',').map(f => f.trim());
		this.dateFields = customDateFields || envDateFields || ['date', 'date_start', 'day', 'date_stop'];

		console.log(`📅 Using date fields: ${this.dateFields.join(', ')}`);
	}

	/**
	 * Собирает все уникальные поля из записей
	 */
	collectAllFields(records: Record<string, any>[]): string[] {
		const fields = new Set<string>();

		for (const record of records) {
			Object.keys(record).forEach(key => {
				if (key !== '__rowNumber') { // Исключаем служебные поля
					fields.add(key);
				}
			});
		}

		return Array.from(fields);
	}

	/**
	 * Объединяет существующие колонки с новыми (новые в конце)
	 */
	mergeColumns(existingColumns: string[], newFields: string[]): string[] {
		const result = [...existingColumns];

		for (const field of newFields) {
			if (!result.includes(field)) {
				result.push(field);
			}
		}

		return result;
	}

	/**
	 * Преобразует массив записей в строки для Google Sheets
	 */
	recordsToRows(records: Record<string, any>[], columns: string[]): any[][] {
		return records.map(record =>
			columns.map(column => this.processFieldValue(record[column], column))
		);
	}

	/**
	 * Обрабатывает значение поля с учетом его типа
	 */
	private processFieldValue(value: any, columnName: string): any {
		if (value == null || value === '') {
			return '';
		}

		// Специальная обработка дат - всегда в формате YYYY-MM-DD
		if (this.isDateField(columnName)) {
			const normalizedDate = this.normalizeDate(String(value));
			return normalizedDate || String(value);
		}

		// Обработка вложенных объектов и массивов
		if (Array.isArray(value) || (typeof value === 'object' && value !== null)) {
			return JSON.stringify(value);
		}

		// Обработка чисел - оставляем как числа
		if (typeof value === 'number') {
			return value;
		}

		// Обработка булевых значений
		if (typeof value === 'boolean') {
			return value;
		}

		// Все остальное как строка
		return String(value);
	}

	/**
	 * Проверяет, является ли поле датой
	 */
	private isDateField(columnName: string): boolean {
		return this.dateFields.includes(columnName.toLowerCase());
	}

	/**
	 * Нормализует дату в формат YYYY-MM-DD
	 */
	normalizeDate(dateString: string): string {
		if (!dateString) return '';

		try {
			const date = new Date(dateString);
			if (isNaN(date.getTime())) {
				return '';
			}

			// Форматируем в YYYY-MM-DD
			return date.toISOString().split('T')[0];
		} catch {
			return '';
		}
	}

	/**
	 * Извлекает даты из записей (с проверкой на наличие дат)
	 */
	extractDatesFromRecords(records: Record<string, any>[]): string[] {
		const dates = new Set<string>();
		let foundDateFields = false;

		for (const record of records) {
			const date = this.getRecordDate(record);
			if (date) {
				foundDateFields = true;
				const normalizedDate = this.normalizeDate(date);
				if (normalizedDate) {
					dates.add(normalizedDate);
				}
			}
		}

		if (!foundDateFields && records.length > 0) {
			console.warn(`⚠️  No date fields found in records. Available fields: ${Object.keys(records[0]).join(', ')}`);
			console.warn(`⚠️  Expected date fields: ${this.dateFields.join(', ')}`);
			console.warn(`⚠️  Incremental mode will append all new data without cleanup`);
		}

		return Array.from(dates).sort();
	}

	/**
	 * Получает дату из записи (ищет в стандартных полях)
	 */
	getRecordDate(record: Record<string, any>): string {
		for (const field of this.dateFields) {
			if (record[field]) {
				return String(record[field]);
			}
		}

		return '';
	}

	/**
	 * Преобразует строки Google Sheets обратно в записи
	 */
	rowsToRecords(rows: string[][], headers: string[]): ProcessedRecord[] {
		const records: ProcessedRecord[] = [];

		for (let i = 1; i < rows.length; i++) { // Пропускаем заголовки
			const row = rows[i];

			// Пропускаем полностью пустые строки
			if (!row || row.every((cell: any) => !cell || cell.toString().trim() === '')) {
				continue;
			}

			const record: Record<string, any> = {};

			headers.forEach((header, index) => {
				record[header] = row[index] || '';
			});

			records.push({
				data: record,
				rowNumber: i + 1 // Google Sheets 1-indexed
			});
		}

		return records;
	}

	/**
	 * Находит индексы колонок с датами
	 */
	findDateColumnIndices(columns: string[]): number[] {
		const indices: number[] = [];

		columns.forEach((column, index) => {
			if (this.isDateField(column)) {
				indices.push(index);
			}
		});

		return indices;
	}

	/**
	 * Группирует последовательные номера в диапазоны
	 */
	groupConsecutiveRanges(numbers: number[]): Array<{start: number, end: number}> {
		if (numbers.length === 0) return [];

		const sorted = [...numbers].sort((a, b) => b - a); // Удаляем с конца
		const ranges: Array<{start: number, end: number}> = [];

		let start = sorted[0];
		let end = sorted[0];

		for (let i = 1; i < sorted.length; i++) {
			if (sorted[i] === end - 1) {
				// Продолжаем диапазон
				end = sorted[i];
			} else {
				// Заканчиваем текущий диапазон
				ranges.push({ start: end, end: start });
				start = sorted[i];
				end = sorted[i];
			}
		}

		// Добавляем последний диапазон
		ranges.push({ start: end, end: start });

		return ranges;
	}
}
