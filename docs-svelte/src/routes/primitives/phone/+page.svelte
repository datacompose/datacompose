<script lang="ts">
	import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "$lib/components/ui/card";

	const examples = [
		{
			title: "Phone Number Standardization",
			description: "Clean and format phone numbers",
			code: `from datacompose import clean_phone_numbers

@clean_phone_numbers.compose()
def standardize_phones(df):
    return (df
        .clean_phone_numbers("phone")
        .format_phone_numbers("phone", "formatted_phone")
        .validate_phone_numbers("formatted_phone")
        .extract_country_code("formatted_phone", "country_code")
    )`
		},
		{
			title: "International Phone Validation",
			description: "Handle international phone numbers",
			code: `@clean_phone_numbers.compose()
def international_phones(df):
    return (df
        .parse_international_numbers("phone")
        .validate_international_format("phone")
        .extract_country_from_phone("phone", "country")
        .format_e164("phone", "e164_format")
    )`
		},
		{
			title: "Mobile vs Landline Detection",
			description: "Classify phone number types",
			code: `@clean_phone_numbers.compose()
def classify_phones(df):
    return (df
        .clean_phone_numbers("phone")
        .detect_phone_type("phone", "phone_type")
        .flag_mobile_numbers("phone", "is_mobile")
        .validate_carrier("phone", "carrier")
    )`
		}
	];

	const methods = [
		{ name: "clean_phone_numbers", description: "Remove special characters and spaces" },
		{ name: "format_phone_numbers", description: "Apply consistent formatting" },
		{ name: "validate_phone_numbers", description: "Check if number is valid" },
		{ name: "extract_country_code", description: "Extract country calling code" },
		{ name: "extract_area_code", description: "Extract area/region code" },
		{ name: "format_e164", description: "Format to E.164 international standard" },
		{ name: "detect_phone_type", description: "Identify mobile vs landline" },
		{ name: "validate_carrier", description: "Validate carrier information" },
		{ name: "normalize_extensions", description: "Handle phone extensions" },
		{ name: "flag_toll_free", description: "Identify toll-free numbers" }
	];
</script>

<div class="space-y-8">
	<header>
		<h1 class="text-4xl font-bold mb-4">Phone Primitives</h1>
		<p class="text-lg text-muted-foreground">
			Complete phone number validation, formatting, and analysis for PySpark DataFrames.
		</p>
	</header>

	<section class="space-y-4">
		<h2 class="text-2xl font-semibold">Installation</h2>
		<pre class="bg-accent rounded-lg p-4 overflow-x-auto"><code>pip install datacompose-phone-primitives</code></pre>
	</section>

	<section class="space-y-4">
		<h2 class="text-2xl font-semibold">Import</h2>
		<pre class="bg-accent rounded-lg p-4 overflow-x-auto"><code>from datacompose import clean_phone_numbers</code></pre>
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
				<CardTitle>Custom Phone Number Validation</CardTitle>
				<CardDescription>Create region-specific phone validation rules</CardDescription>
			</CardHeader>
			<CardContent>
				<pre class="bg-accent rounded-md p-4 overflow-x-auto text-sm"><code>{`from pyspark.sql import functions as F
from datacompose import clean_phone_numbers

@clean_phone_numbers.register_primitive
def validate_us_mobile(df, phone_col):
    """Validate US mobile numbers with specific area codes"""
    mobile_area_codes = ['415', '650', '408', '510']
    
    return df.withColumn(
        f"{phone_col}_is_us_mobile",
        F.when(
            F.substring(F.col(phone_col), 1, 3).isin(mobile_area_codes),
            True
        ).otherwise(False)
    )

# Use in pipeline
@clean_phone_numbers.compose()
def us_mobile_pipeline(df):
    return (df
        .clean_phone_numbers("phone")
        .validate_us_mobile("phone")
        .filter(F.col("phone_is_us_mobile"))
    )`}</code></pre>
			</CardContent>
		</Card>
	</section>
</div>