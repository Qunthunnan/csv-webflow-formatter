const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");
const { parse } = require("json2csv");
const slugify = require("slugify");

const inputDir = path.join(__dirname, "input");
const outputDir = path.join(__dirname, "output");

if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir);

const slugifyOptions = {
	lower: true,
	strict: true,
	trim: true,
};

// Config schema for each collection
const collectionSchema = {
	Stations: {
		keep: [
			"SiteID",
			"Site Name",
			"Latitude",
			"Longitude",
			"Site Description",
			"Monitoring Info",
			"Waterbody",
			"Watersheds (from Waterbody) 2",
			"Monitoring Organization",
			"Most Recent Ecoli Reading",
			"Most Recent Sample Date",
		],
		slugSource: "SiteID",
		singleRefFields: ["Waterbody", "Watersheds (from Waterbody) 2"],
		multiRefFields: ["Monitoring Organization"],
	},
	Waterbodies: {
		keep: ["Name", "Watersheds", "Monitoring Sites"],
		slugSource: "Name",
		singleRefFields: ["Watersheds"],
		multiRefFields: ["Monitoring Sites"],
	},
	Watersheds: {
		keep: ["Name", "Waterbodies", "Monitoring Sites (from Waterbodies)"],
		slugSource: "Name",
		multiRefFields: ["Waterbodies", "Monitoring Sites (from Waterbodies)"],
	},
	Organizations: {
		keep: [
			"Organization Name",
			"Logo",
			"Organization Website",
			"Organization Description",
			"Monitoring Sites",
		],
		slugSource: "Organization Name",
		multiRefFields: ["Monitoring Sites"],
	},
};

// Slug maps for links between collections
const slugMaps = {};

// Reading CSV to array of objects
function readCSV(filePath) {
	return new Promise((resolve) => {
		const results = [];
		fs.createReadStream(filePath)
			.pipe(csv())
			.on("data", (data) => results.push(data))
			.on("end", () => resolve(results));
	});
}

// Writing CSV
function writeCSV(fileName, data) {
	const csvData = parse(data);
	fs.writeFileSync(path.join(outputDir, fileName), csvData, "utf8");
}

// Parsing line from CSV
function parseCsvMultiValue(cell) {
	if (!cell) return [];

	const results = [];
	let current = "";
	let inQuotes = false;

	for (let i = 0; i < cell.length; i++) {
		const char = cell[i];
		const nextChar = cell[i + 1];

		if (char === '"') {
			inQuotes = !inQuotes;
			continue;
		}

		if (char === "," && !inQuotes) {
			if (current.trim()) results.push(current.trim());
			current = "";
		} else {
			current += char;
		}
	}

	if (current.trim()) results.push(current.trim());

	return results;
}

// Main logic to process CSV files
async function processCSVFiles() {
	const files = fs.readdirSync(inputDir).filter((f) => f.endsWith(".csv"));
	const datasets = {};

	// 1. CSV reading + slug generation
	for (const file of files) {
		const baseName = path.basename(file).split("-")[0]; // Stations, Watersheds
		const data = await readCSV(path.join(inputDir, file));
		datasets[baseName] = data;

		const schema = collectionSchema[baseName];
		if (!schema) continue;

		const slugMap = {};
		for (const row of data) {
			// Normalize keys to remove extra whitespace from csv headers
			const normalizedRow = {};
			for (const key in row) {
				normalizedRow[key.trim()] = row[key];
			}
			const sourceValue = normalizedRow[schema.slugSource];
			if (sourceValue) {
				const slug = slugify(sourceValue, slugifyOptions);
				normalizedRow.slug = slug;
				slugMap[sourceValue.trim()] = slug;
			}
			// Replace original row with normalized row
			Object.assign(row, normalizedRow);
		}
		slugMaps[baseName] = slugMap;
	}

	// 2. Processing each CSV
	for (const [collection, rows] of Object.entries(datasets)) {
		const schema = collectionSchema[collection];
		if (!schema) continue;

		const newRows = [];

		for (const row of rows) {
			const newRow = {};

			for (const key of schema.keep) {
				if (schema.multiRefFields.includes(key)) {
					const targetCollection = detectReferenceCollection(key);
					const refSlugs = parseCsvMultiValue(row[key])
						.map(
							(item) =>
								slugMaps[targetCollection]?.[item] ||
								slugify(item, slugifyOptions),
						)
						.filter(Boolean);
					newRow[key] = refSlugs.join(";");
				} else if (schema.singleRefFields?.includes(key)) {
					const targetCollection = detectReferenceCollection(key);
					const ref = row[key]?.trim();
					const slug = ref
						? slugMaps[targetCollection]?.[ref] ||
						  slugify(ref, slugifyOptions)
						: "";
					newRow[key] = slug;
				} else {
					newRow[key] = row[key] || "";
				}
			}

			// Always add slug
			const sourceValue = row[schema.slugSource];
			newRow.slug = slugify(sourceValue || "", slugifyOptions);

			newRows.push(newRow);
		}

		writeCSV(`${collection}.csv`, newRows);
	}

	console.log("CSV files processed and saved in /output");
}

// 🔎 Automatic collection detection by field
function detectReferenceCollection(fieldName) {
	const fieldToCollectionMap = {
		"Monitoring Organization": "Organizations",
		"Monitoring Sites": "Stations",
		"Monitoring Sites (from Waterbodies)": "Stations",
		Waterbodies: "Waterbodies",
		Watersheds: "Watersheds",
		Sites: "Stations",
	};
	return fieldToCollectionMap[fieldName] || "Unknown";
}

processCSVFiles();
