<script lang="ts">
	import "../app.css";
	import AppSidebar from "$lib/components/app-sidebar.svelte";
	import TableOfContents from "$lib/components/table-of-contents.svelte";
	import favicon from '$lib/assets/favicon.svg';
	import { page } from '$app/state';
	import { Search, Sun, Moon } from 'lucide-svelte';
	import { Button } from "$lib/components/ui/button";
	import * as Sidebar from "$lib/components/ui/sidebar";
	
	let { children } = $props();
	let isDark = $state(true);
	
	$effect(() => {
		if (isDark) {
			document.documentElement.classList.add('dark');
		} else {
			document.documentElement.classList.remove('dark');
		}
	});
</script>

<svelte:head>
	<link rel="icon" href={favicon} />
</svelte:head>

<Sidebar.Provider open={false}>
<div class="min-h-screen bg-background">
	<!-- Header -->
	<header class="sticky top-0 z-50 w-full border-b border-border bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
		<div class="flex h-16 items-center px-4 md:px-6">
			<!-- Sidebar trigger -->
			<Sidebar.Trigger class="mr-2 text-foreground hover:bg-accent" />

			<!-- Logo -->
			<a href="/" class="flex items-center space-x-2 mr-6">
				<span class="font-bold text-xl">DataCompose</span>
			</a>

			<!-- Navigation -->
			<nav class="hidden md:flex items-center space-x-6 text-sm">
				<a href="/docs" class="text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-100 transition-colors">
					Docs
				</a>
				<a href="/primitives/emails" class="text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-100 transition-colors">
					Primitives
				</a>
				<a href="/examples" class="text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-100 transition-colors">
					Examples
				</a>
				<a href="/blog" class="text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-100 transition-colors">
					Blog
				</a>
			</nav>

			<div class="flex-1" />

			<!-- Right side buttons -->
			<div class="flex items-center space-x-2">
				<!-- Search button -->
				<Button variant="ghost" size="icon" class="text-gray-600 dark:text-gray-400">
					<Search class="h-5 w-5" />
				</Button>

				<!-- Theme toggle -->
				<Button 
					variant="ghost" 
					size="icon"
					onclick={() => isDark = !isDark}
					class="text-gray-600 dark:text-gray-400"
				>
					{#if isDark}
						<Sun class="h-5 w-5" />
					{:else}
						<Moon class="h-5 w-5" />
					{/if}
				</Button>

				<!-- GitHub -->
				<Button variant="ghost" size="icon" class="text-gray-600 dark:text-gray-400">
				</Button>
			</div>
		</div>
	</header>

	<!-- Main content with sidebar -->
	<div class="flex h-[calc(100vh-4rem)]">
		<AppSidebar />
		
		<Sidebar.Inset>
			<div class="flex flex-1">
				<!-- Main content -->
				<main class="flex-1 min-w-0">
					<article class="mx-auto max-w-4xl px-4 md:px-8 py-8 md:py-12">
						{@render children?.()}
					</article>
				</main>

				<!-- Right sidebar - Table of contents -->
				{#if page.url.pathname.startsWith('/primitives')}
				<aside class="hidden xl:block w-64 border-l border-border h-full sticky top-0 overflow-y-auto">
					<TableOfContents />
				</aside>
				{/if}
			</div>
		</Sidebar.Inset>
	</div>
</div>
</Sidebar.Provider>

<style>
	:global(html) {
		scroll-behavior: smooth;
	}
	
	:global(.dark) {
		color-scheme: dark;
	}
</style>
