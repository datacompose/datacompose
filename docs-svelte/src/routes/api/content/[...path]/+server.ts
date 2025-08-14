import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import fs from 'fs';
import path from 'path';

export const GET: RequestHandler = async ({ params }) => {
	const filePath = params.path;
	
	// Security: prevent directory traversal
	if (filePath?.includes('..')) {
		return json({ error: 'Invalid path' }, { status: 400 });
	}
	
	try {
		// Construct the full path to the markdown file
		const fullPath = path.join(process.cwd(), 'content', filePath);
		
		// Check if file exists and is a markdown file
		if (!fullPath.endsWith('.md')) {
			return json({ error: 'Only markdown files are allowed' }, { status: 400 });
		}
		
		// Read the file
		const content = fs.readFileSync(fullPath, 'utf-8');
		
		// Return the content
		return new Response(content, {
			headers: {
				'Content-Type': 'text/markdown',
				'Cache-Control': 'public, max-age=3600'
			}
		});
	} catch (error) {
		console.error('Error reading file:', error);
		return json({ error: 'File not found' }, { status: 404 });
	}
};