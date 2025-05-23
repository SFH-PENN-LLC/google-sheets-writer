/**
 * Универсальная retry логика для Google Sheets API
 * Поддерживает exponential backoff и умную обработку ошибок
 */

export interface RetryConfig {
	maxRetries: number;
	baseDelay: number;
	maxDelay: number;
}

const DEFAULT_RETRY_CONFIG: RetryConfig = {
	maxRetries: 3,
	baseDelay: 1000,
	maxDelay: 10000
};

/**
 * Выполняет операцию с retry логикой
 */
export async function retryOperation<T>(
	operation: () => Promise<T>,
	config: RetryConfig = DEFAULT_RETRY_CONFIG
): Promise<T> {
	let lastError: any;

	for (let attempt = 0; attempt <= config.maxRetries; attempt++) {
		try {
			if (attempt > 0) {
				const delay = calculateRetryDelay(attempt - 1, config);
				console.log(`🔄 Retry attempt ${attempt}/${config.maxRetries} after ${delay}ms...`);
				await sleep(delay);
			}

			return await operation();
		} catch (error: any) {
			lastError = error;

			// Проверяем, стоит ли повторять
			if (!shouldRetry(error) || attempt >= config.maxRetries) {
				break;
			}

			console.warn(`⚠️  Google Sheets API error (attempt ${attempt + 1}):`, error.message);
		}
	}

	throw new Error(`Operation failed after ${config.maxRetries + 1} attempts. Last error: ${lastError.message}`);
}

/**
 * Вычисляет задержку для retry с exponential backoff и jitter
 */
function calculateRetryDelay(attempt: number, config: RetryConfig): number {
	// Exponential backoff
	const exponentialDelay = Math.min(
		config.baseDelay * Math.pow(2, attempt),
		config.maxDelay
	);

	// Добавляем случайный jitter ±25% для избежания thundering herd
	const jitter = exponentialDelay * 0.25 * (Math.random() * 2 - 1);
	return Math.max(config.baseDelay, exponentialDelay + jitter);
}

/**
 * Определяет, стоит ли повторять операцию
 */
function shouldRetry(error: any): boolean {
	// Retry на временные ошибки Google API
	const retryableErrors = [
		'RATE_LIMIT_EXCEEDED',
		'QUOTA_EXCEEDED',
		'INTERNAL_ERROR',
		'BACKEND_ERROR',
		'SERVICE_UNAVAILABLE',
		'TIMEOUT'
	];

	const retryableHttpCodes = [429, 500, 502, 503, 504];

	// Проверяем HTTP коды
	if (error.code && retryableHttpCodes.includes(error.code)) {
		return true;
	}

	if (error.status && retryableHttpCodes.includes(error.status)) {
		return true;
	}

	// Проверяем сообщения об ошибках
	if (error.message) {
		const message = error.message.toUpperCase();
		return retryableErrors.some(retryable => message.includes(retryable));
	}

	// Сетевые ошибки
	if (error.code && ['ECONNRESET', 'ETIMEDOUT', 'ENOTFOUND', 'EAI_AGAIN'].includes(error.code)) {
		return true;
	}

	return false;
}

/**
 * Простая функция задержки
 */
function sleep(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms));
}
