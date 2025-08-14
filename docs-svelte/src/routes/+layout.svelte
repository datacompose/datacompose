<script lang="ts">
	import "../app.css";
	import * as Sidebar from "$lib/components/ui/sidebar";
	import AppSidebar from "$lib/components/app-sidebar.svelte";
	import favicon from '$lib/assets/favicon.svg';

	let { children } = $props();
</script>

<svelte:head>
	<link rel="icon" href={favicon} />
</svelte:head>

<Sidebar.Provider>
	<div class="flex min-h-screen w-full">
		<AppSidebar />
		<div class="flex-1">
			<header class="sticky top-0 z-10 flex h-14 items-center gap-4 border-b bg-background px-6">
				<Sidebar.Trigger class="-ml-2" />
				<nav class="flex items-center gap-6 text-sm">
					<a href="/" class="font-semibold">Home</a>
					<a href="/concepts" class="text-muted-foreground hover:text-foreground transition-colors">Core Concepts</a>
					<a href="/getting-started" class="text-muted-foreground hover:text-foreground transition-colors">Getting Started</a>
					<a href="/examples" class="text-muted-foreground hover:text-foreground transition-colors">Examples</a>
				</nav>
				<div class="flex-1" />
				<a 
					href="https://github.com/datacompose" 
					target="_blank"
					rel="noopener noreferrer"
					class="text-muted-foreground hover:text-foreground transition-colors"
				>
					GitHub
				</a>
			</header>
			<main class="flex-1">
				<div class="container py-6">
					{@render children?.()}
				</div>
			</main>
		</div>
	</div>
</Sidebar.Provider>
