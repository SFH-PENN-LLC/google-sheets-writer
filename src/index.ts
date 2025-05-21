import { google } from 'googleapis';
import fs   from 'fs/promises';

const sheetId  = process.env.GOOGLE_SHEET_ID!;
const creds    = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON!);
const sheetName= process.env.SHEET_NAME!;
const full     = process.env.FULL_REFRESH === 'true';
const path     = process.argv[2] || 'data/data.json';

const auth = new google.auth.JWT(
	creds.client_email,
	undefined,
	creds.private_key,
	[
		'https://www.googleapis.com/auth/spreadsheets',
	]
);

const sheets = google.sheets({ version: 'v4', auth });

async function main(): Promise<void> {
	const raw     = await fs.readFile(path, 'utf-8');
	const records = JSON.parse(raw) as Record<string, unknown>[];

	if (records.length === 0) {
    await sheets.spreadsheets.values.update({
      spreadsheetId:  sheetId,
      range:          `${sheetName}!A1`,
      valueInputOption: 'RAW',
      requestBody:     { values: [['there is no data']] }
    });
    console.log('No data → wrote message to A1');
    return;
  }

	const headers = Object.keys(records[0]);

	const values = records.map((r) =>
		headers.map((h) => (r[h] === undefined ? '' : String(r[h])))
	);

	if (full) {
		// Полная перезапись листа

		await sheets.spreadsheets.values.clear({
			spreadsheetId: sheetId,
			range: `${sheetName}!A:Z`,
		});

		await sheets.spreadsheets.values.update({
			spreadsheetId:  sheetId,
			range:          `${sheetName}!A1`,
			valueInputOption:'RAW',
			requestBody:    { values: [headers, ...values] },
		});

		console.log(`Full refresh: wrote ${records.length} rows to "${sheetName}".`);

	} else {
		// Инкрементная загрузка: удаляем старые строки за сегодня и дописываем

		const dateCol = headers.indexOf('date') >= 0 ? headers.indexOf('date') : 0;
		const today   = new Date().toISOString().slice(0, 10);

		const existing = await sheets.spreadsheets.values.get({
			spreadsheetId: sheetId,
			range:         sheetName,
		});

		const rows   = existing.data.values || [];
		const clears: Promise<any>[] = [];

		for (let i = 1; i < rows.length; i++) {
			if (rows[i][dateCol] === today) {
				const rowNumber = i + 1;
				clears.push(
					sheets.spreadsheets.values.clear({
						spreadsheetId: sheetId,
						range:         `${sheetName}!A${rowNumber}:Z${rowNumber}`,
					})
				);
			}
		}

		await Promise.all(clears);

		await sheets.spreadsheets.values.append({
			spreadsheetId:  sheetId,
			range:          sheetName,
			valueInputOption:'RAW',
			requestBody:    { values },
		});

		console.log(`Incremental: appended ${records.length} rows for date ${today}.`);
	}
}

main();
