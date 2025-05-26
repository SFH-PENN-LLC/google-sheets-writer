/**
 * Маппер для преобразования JSON данных в строки Google Sheets
 * ИСПРАВЛЕНО: полагается на предобработку данных, всегда ищет поле "date"
 */

export interface ProcessedRecord {
	data: Record<string, any>;
	rowNumber?: number;
}

export class RecordMapper {
	private dateFields: string[];

	constructor() {
		// ИСПРАВЛЕНО: полагаемся на предобработку данных
		// DateStandardizer уже унифицировал все поля дат в "date"
		this.dateFields = ['date'];

		console.log(`📅 Using date fields: ${this.dateFields.join(', ')}`);
		console.log(`🔧 Relying on data preprocessing (DateStandardizer) for date field standardization`);
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

		// Специальная обработка дат - возвращаем строку YYYY-MM-DD
		if (this.isDateField(columnName)) {
			const normalized = this.normalizeDate(String(value));
			return normalized || String(value);
		}

		// Обработка вложенных объектов и массивов
		if (Array.isArray(value) || (typeof value === 'object' && value !== null)) {
			return JSON.stringify(value);
		}

		// Обработка чисел
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
			const cleaned = dateString.trim();

			// Если уже в формате YYYY-MM-DD, проверяем валидность
			if (/^\d{4}-\d{2}-\d{2}$/.test(cleaned)) {
				const date = new Date(cleaned + 'T00:00:00.000Z');
				if (!isNaN(date.getTime())) {
					return cleaned;
				}
			}

			// Парсим различные форматы
			const date = new Date(cleaned);
			if (isNaN(date.getTime())) {
				return '';
			}

			// Форматируем в YYYY-MM-DD
			const year = date.getFullYear();
			const month = String(date.getMonth() + 1).padStart(2, '0');
			const day = String(date.getDate()).padStart(2, '0');

			return `${year}-${month}-${day}`;
		} catch {
			return '';
		}
	}

	/**
	 * Извлекает даты из записей
	 * ИСПРАВЛЕНО: упрощенная логика, полагается на предобработку
	 */
	extractDatesFromRecords(records: Record<string, any>[]): string[] {
		const dates = new Set<string>();
		let foundDateFields = false;

		console.log(`🔍 Looking for date field: "date" (standardized by preprocessing)`);

		for (const record of records) {
			const date = this.getRecordDate(record);
			if (date) {
				foundDateFields = true;
				const normalizedDate = this.normalizeDate(date);
				if (normalizedDate) {
					dates.add(normalizedDate);
					console.log(`📅 Found date: ${date} -> ${normalizedDate}`);
				}
			}
		}

		if (!foundDateFields && records.length > 0) {
			console.error(`❌ CRITICAL: No "date" field found in records!`);
			console.error(`   Available fields: ${Object.keys(records[0]).join(', ')}`);
			console.error(`   Expected field: "date"`);
			console.error(`   🔧 Check data preprocessing (DateStandardizer) - it should create "date" field`);
			console.error(`   ⚠️  Incremental mode will append all new data without cleanup`);
		} else if (foundDateFields) {
			console.log(`✅ Found ${dates.size} unique dates: ${Array.from(dates).join(', ')}`);
		}

		return Array.from(dates).sort();
	}

	/**
	 * Получает дату из записи
	 * ИСПРАВЛЕНО: ищет только поле "date"
	 */
	getRecordDate(record: Record<string, any>): string {
		// Полагаемся на предобработку - ищем только "date"
		if (record.date) {
			return String(record.date);
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
				let cellValue = row[index] || '';

				// Удаляем апострофы из дат при чтении
				if (typeof cellValue === 'string' && cellValue.startsWith("'")) {
					cellValue = cellValue.substring(1);
				}

				record[header] = cellValue;
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

		const sorted = [...numbers].sort((a, b) => b - a);
		const ranges: Array<{start: number, end: number}> = [];

		let start = sorted[0];
		let end = sorted[0];

		for (let i = 1; i < sorted.length; i++) {
			if (sorted[i] === end - 1) {
				end = sorted[i];
			} else {
				ranges.push({ start: end, end: start });
				start = sorted[i];
				end = sorted[i];
			}
		}

		ranges.push({ start: end, end: start });

		return ranges;
	}

	/**
	 * Диагностический метод для проверки данных
	 */
	validatePreprocessing(records: Record<string, any>[]): boolean {
		if (records.length === 0) {
			console.warn('⚠️  No records to validate');
			return true;
		}

		const firstRecord = records[0];
		const hasDateField = firstRecord.hasOwnProperty('date');

		console.log('\n🔍 PREPROCESSING VALIDATION:');
		console.log(`   Records count: ${records.length}`);
		console.log(`   Has "date" field: ${hasDateField ? '✅ YES' : '❌ NO'}`);

		if (hasDateField) {
			console.log(`   Sample date value: "${firstRecord.date}"`);
		} else {
			console.log(`   Available fields: ${Object.keys(firstRecord).join(', ')}`);
			console.error('❌ DateStandardizer failed to create "date" field!');
			return false;
		}

		// Проверяем все записи
		const recordsWithoutDate = records.filter(r => !r.date);
		if (recordsWithoutDate.length > 0) {
			console.error(`❌ ${recordsWithoutDate.length} records missing "date" field`);
			return false;
		}

		console.log('✅ All records have "date" field - preprocessing worked correctly');
		return true;
	}
}
