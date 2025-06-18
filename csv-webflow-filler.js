const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");
const { parse } = require("json2csv");

const INPUT_DIR = "./input";
const OUTPUT_DIR = "./output";

// Load CSV files into memory
function loadCSV(filename) {
	return new Promise((resolve, reject) => {
		const results = [];
		fs.createReadStream(path.join(INPUT_DIR, filename))
			.pipe(csv())
			.on("data", (data) => results.push(data))
			.on("end", () => resolve(results))
			.on("error", reject);
	});
}

// Save updated CSV
function saveCSV(filename, data) {
	const csvData = parse(data, { quote: '"' });
	fs.writeFileSync(path.join(OUTPUT_DIR, filename), csvData, "utf8");
}

(async () => {
	const [sites, orgs, waterbodies, watersheds] = await Promise.all([
		loadCSV("Is it clean_ - Sites.csv"),
		loadCSV("Is it clean_ - Monitoring organizations.csv"),
		loadCSV("Is it clean_ - Waterbodies.csv"),
		loadCSV("Is it clean_ - Watersheds (1).csv"),
	]);

	// Map slugs by related fields
	const groupByField = (data, groupField, valueField) => {
		const map = {};
		data.forEach((item) => {
			const key = item[groupField];
			if (key) {
				if (!map[key]) map[key] = [];
				map[key].push(item[valueField]);
			}
		});
		Object.keys(map).forEach(
			(key) => (map[key] = [...new Set(map[key])].sort().join("; ")),
		);
		return map;
	};

	const orgToSites = groupByField(sites, "Monitoring organizations", "Slug");
	const waterbodyToSites = groupByField(sites, "Waterbody", "Slug");
	const watershedToSites = groupByField(sites, "Watershed", "Slug");
	const watershedToWaterbodies = groupByField(
		waterbodies,
		"Watershed",
		"Slug",
	);

	// Apply mappings
	orgs.forEach((org) => (org["Sites"] = orgToSites[org["Slug"]] || ""));
	waterbodies.forEach(
		(wb) => (wb["Sites"] = waterbodyToSites[wb["Slug"]] || ""),
	);
	watersheds.forEach((ws) => {
		ws["Sites"] = watershedToSites[ws["Slug"]] || "";
		ws["Waterbodies"] = watershedToWaterbodies[ws["Slug"]] || "";
	});

	// Ensure output directory exists
	if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR);

	// Save files
	saveCSV("Is it clean_ - Monitoring organizations.csv", orgs);
	saveCSV("Is it clean_ - Waterbodies.csv", waterbodies);
	saveCSV("Is it clean_ - Watersheds (1).csv", watersheds);

	console.log("Data successfully populated and saved to /output.");
})();
