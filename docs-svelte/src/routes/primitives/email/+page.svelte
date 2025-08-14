<script lang="ts">
	import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "$lib/components/ui/card";

	const examples = [
		{
			title: "Basic Email Cleaning",
			description: "Clean and validate email addresses",
			code: `from datacompose import clean_emails

@clean_emails.compose()
def clean_email_pipeline(df):
    return (df
        .clean_emails("email")
        .validate_emails("email")
        .remove_invalid_emails("email")
    )`
		},
		{
			title: "Domain Extraction",
			description: "Extract and analyze email domains",
			code: `@clean_emails.compose()
def domain_analysis(df):
    return (df
        .extract_email_domains("email", "domain")
        .get_email_providers("email", "provider")
        .flag_disposable_emails("email", "is_disposable")
    )`
		},
		{
			title: "Corporate Email Filtering",
			description: "Filter for corporate email addresses",
			code: `@clean_emails.compose()
def corporate_emails(df):
    return (df
        .clean_emails("email")
        .filter_corporate_emails("email")
        .extract_company_from_email("email", "company")
        .validate_corporate_domains("email", "is_valid_corp")
    )`
		}
	];

	const methods = [
		{ name: "clean_emails", description: "Remove whitespace and standardize format" },
		{ name: "validate_emails", description: "Check if email format is valid" },
		{ name: "extract_email_domains", description: "Extract domain from email address" },
		{ name: "remove_invalid_emails", description: "Filter out invalid email addresses" },
		{ name: "normalize_emails", description: "Convert to lowercase and trim" },
		{ name: "get_email_providers", description: "Identify email service provider" },
		{ name: "flag_disposable_emails", description: "Mark temporary email addresses" },
		{ name: "filter_corporate_emails", description: "Keep only business emails" },
		{ name: "extract_username", description: "Get username part of email" },
		{ name: "validate_mx_records", description: "Check domain MX records" }
	];
</script>

<div class="space-y-8">
	<header>
		<h1 class="text-4xl font-bold mb-4">Email Primitives</h1>
		<p class="text-lg text-muted-foreground">
			Comprehensive email cleaning, validation, and transformation functions for PySpark DataFrames.
		</p>
	</header>

	<section class="space-y-4">
		<h2 class="text-2xl font-semibold">Installation</h2>
		<pre class="bg-accent rounded-lg p-4 overflow-x-auto"><code>pip install datacompose-email-primitives</code></pre>
	</section>

	<section class="space-y-4">
		<h2 class="text-2xl font-semibold">Import</h2>
		<pre class="bg-accent rounded-lg p-4 overflow-x-auto"><code>from datacompose import clean_emails</code></pre>
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
				<CardTitle>Creating Your Own Email Primitive</CardTitle>
				<CardDescription>Extend the library with custom validation logic</CardDescription>
			</CardHeader>
			<CardContent>
				<pre class="bg-accent rounded-md p-4 overflow-x-auto text-sm"><code>{`from pyspark.sql import functions as F
from datacompose import clean_emails

@clean_emails.register_primitive
def validate_company_emails(df, email_col, company_domain):
    """Validate emails belong to specific company domain"""
    return df.filter(
        F.col(email_col).endswith(f"@{company_domain}")
    )

# Use in pipeline
@clean_emails.compose()
def company_email_pipeline(df):
    return (df
        .clean_emails("email")
        .validate_company_emails("email", "acme.com")
    )`}</code></pre>
			</CardContent>
		</Card>
	</section>
</div>