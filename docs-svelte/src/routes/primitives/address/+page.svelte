<script lang="ts">
	import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "$lib/components/ui/card";

	const examples = [
		{
			title: "Address Standardization",
			description: "Clean and standardize physical addresses",
			code: `from datacompose import clean_addresses

@clean_addresses.compose()
def standardize_addresses(df):
    return (df
        .parse_address_components("address_line")
        .standardize_street_types("street")
        .validate_zip_codes("zip_code")
        .format_addresses("formatted_address")
    )`
		},
		{
			title: "Geocoding Pipeline",
			description: "Validate and geocode addresses",
			code: `@clean_addresses.compose()
def geocoding_pipeline(df):
    return (df
        .clean_addresses("full_address")
        .validate_addresses("full_address")
        .geocode_addresses("full_address", "lat", "lon")
        .calculate_distance_from_point("lat", "lon", 40.7128, -74.0060, "distance_from_nyc")
    )`
		},
		{
			title: "Address Deduplication",
			description: "Find and merge duplicate addresses",
			code: `@clean_addresses.compose()
def dedupe_addresses(df):
    return (df
        .standardize_addresses("address")
        .hash_addresses("address", "address_hash")
        .flag_duplicate_addresses("address_hash", "is_duplicate")
        .merge_duplicate_records("address_hash")
    )`
		}
	];

	const methods = [
		{ name: "clean_addresses", description: "Remove extra spaces and standardize format" },
		{ name: "parse_address_components", description: "Extract street, city, state, zip" },
		{ name: "standardize_street_types", description: "Convert St to Street, Rd to Road, etc." },
		{ name: "validate_zip_codes", description: "Validate ZIP code format and existence" },
		{ name: "validate_addresses", description: "Check if address is deliverable" },
		{ name: "geocode_addresses", description: "Get latitude/longitude coordinates" },
		{ name: "format_addresses", description: "Format to standard mailing format" },
		{ name: "extract_apartment_numbers", description: "Extract unit/apt numbers" },
		{ name: "normalize_state_names", description: "Convert state names to abbreviations" },
		{ name: "validate_po_boxes", description: "Identify and validate PO Box addresses" }
	];
</script>

<div class="space-y-8">
	<header>
		<h1 class="text-4xl font-bold mb-4">Address Primitives</h1>
		<p class="text-lg text-muted-foreground">
			Powerful address parsing, validation, and standardization tools for PySpark DataFrames.
		</p>
	</header>

	<section class="space-y-4">
		<h2 class="text-2xl font-semibold">Installation</h2>
		<pre class="bg-accent rounded-lg p-4 overflow-x-auto"><code>pip install datacompose-address-primitives</code></pre>
	</section>

	<section class="space-y-4">
		<h2 class="text-2xl font-semibold">Import</h2>
		<pre class="bg-accent rounded-lg p-4 overflow-x-auto"><code>from datacompose import clean_addresses</code></pre>
	</section>

	<section class="space-y-4">
		<h2 class="text-2xl font-semibold">Available Methods</h2>
		<div class="grid gap-3">
			{#each methods as method}
				<Card>
					<CardHeader class="pb-3">
						<CardTitle class="text-base font-mono">{method.name}()</CardTitle>
						<CardDescription>{method.description}</CardDescription>
					</CardHeader>
				</Card>
			{/each}
		</div>
	</section>

	<section class="space-y-4">
		<h2 class="text-2xl font-semibold">Pipeline Examples</h2>
		<div class="space-y-6">
			{#each examples as example}
				<Card>
					<CardHeader>
						<CardTitle>{example.title}</CardTitle>
						<CardDescription>{example.description}</CardDescription>
					</CardHeader>
					<CardContent>
						<pre class="bg-accent rounded-md p-4 overflow-x-auto text-sm"><code>{example.code}</code></pre>
					</CardContent>
				</Card>
			{/each}
		</div>
	</section>

	<section class="space-y-4">
		<h2 class="text-2xl font-semibold">Custom Primitives</h2>
		<Card>
			<CardHeader>
				<CardTitle>Building Custom Address Validators</CardTitle>
				<CardDescription>Create region-specific address validation rules</CardDescription>
			</CardHeader>
			<CardContent>
				<pre class="bg-accent rounded-md p-4 overflow-x-auto text-sm"><code>{`from pyspark.sql import functions as F
from datacompose import clean_addresses

@clean_addresses.register_primitive
def validate_canadian_postal_codes(df, postal_col):
    """Validate Canadian postal code format (A1A 1A1)"""
    pattern = r'^[A-Z]\d[A-Z] \d[A-Z]\d$'
    return df.withColumn(
        f"{postal_col}_valid",
        F.regexp_extract(F.upper(F.col(postal_col)), pattern, 0) != ""
    )

# Use in pipeline
@clean_addresses.compose()
def canadian_address_pipeline(df):
    return (df
        .clean_addresses("address")
        .validate_canadian_postal_codes("postal_code")
        .filter(F.col("postal_code_valid"))
    )`}</code></pre>
			</CardContent>
		</Card>
	</section>
</div>