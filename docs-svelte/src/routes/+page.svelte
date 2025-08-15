<script lang="ts">
	import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "$lib/components/ui/card";
	import { Button } from "$lib/components/ui/button";

	const features = [
		{
			title: "Email Primitives",
			description: "Clean, validate, and transform email addresses with PySpark",
			href: "/primitives/email"
		},
		{
			title: "Address Primitives",
			description: "Standardize and validate physical addresses at scale",
			href: "/primitives/address"
		},
		{
			title: "Phone Primitives",
			description: "Format and validate phone numbers from any region",
			href: "/primitives/phone"
		}
	];

	const highlights = [
		{
			title: "PySpark Native",
			description: "Built for distributed processing with Apache Spark"
		},
		{
			title: "Pipeline Ready",
			description: "Use @compose decorator for complex data pipelines"
		},
		{
			title: "Extensible",
			description: "Create custom primitives tailored to your needs"
		},
		{
			title: "Production Ready",
			description: "Battle-tested in production environments"
		}
	];
</script>

<div class="space-y-12">
	<section class="text-center space-y-4">
		<h1 class="text-5xl font-bold tracking-tight">Datacompose</h1>
		<p class="text-xl text-muted-foreground max-w-2xl mx-auto">
			Powerful data primitive libraries for cleaning, validating, and transforming 
			emails, addresses, and phone numbers in PySpark pipelines
		</p>
		<div class="flex gap-4 justify-center pt-4">
			<Button href="/installation" size="lg">Get Started</Button>
			<Button href="/examples/basic" variant="outline" size="lg">View Examples</Button>
		</div>
	</section>

	<section class="space-y-6">
		<h2 class="text-3xl font-bold text-center">Core Primitives</h2>
		<div class="grid md:grid-cols-3 gap-6">
			{#each features as feature}
				<Card>
					<CardHeader>
						<CardTitle>{feature.title}</CardTitle>
						<CardDescription>{feature.description}</CardDescription>
					</CardHeader>
					<CardContent>
						<Button href={feature.href} variant="outline" class="w-full">
							Learn More →
						</Button>
					</CardContent>
				</Card>
			{/each}
		</div>
	</section>

	<section class="space-y-6">
		<h2 class="text-3xl font-bold text-center">Why Datacompose?</h2>
		<div class="grid md:grid-cols-2 gap-4">
			{#each highlights as highlight}
				<Card>
					<CardHeader>
						<CardTitle class="text-lg">{highlight.title}</CardTitle>
						<CardDescription>{highlight.description}</CardDescription>
					</CardHeader>
				</Card>
			{/each}
		</div>
	</section>

	<section class="bg-accent rounded-lg p-8 text-center space-y-4">
		<h2 class="text-2xl font-bold">Quick Example</h2>
		<pre class="bg-background rounded-md p-4 text-left overflow-x-auto"><code>{`from datacompose import clean_emails

@clean_emails.compose()
def email_pipeline(df):
    return (df
        .clean_emails("email_col")
        .validate_emails("email_col")
        .extract_email_domains("email_col", "domain_col")
    )

# Apply to your PySpark DataFrame
cleaned_df = email_pipeline(spark_df)`}</code></pre>
	</section>
</div>
