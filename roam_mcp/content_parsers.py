"""External content parsing operations for the Roam MCP server."""

import os
import tempfile
import logging
from typing import Dict, Any, Optional
import httpx
import trafilatura
from unstructured.partition.pdf import partition_pdf

# Set up logging
logger = logging.getLogger("roam-mcp.content_parsers")

async def parse_webpage(url: str) -> Dict[str, Any]:
    """
    Parse content from a web page URL.
    
    Args:
        url: URL of the webpage to parse
        
    Returns:
        Result with parsed content
    """
    try:
        logger.debug(f"Fetching web page content from: {url}")
        downloaded = trafilatura.fetch_url(url)
        
        if not downloaded:
            return {
                "success": False,
                "error": f"Failed to download content from {url}"
            }
        
        # Extract main content with document structure preserved
        content = trafilatura.extract(
            downloaded,
            output_format='text',
            include_links=False,
            include_formatting=True
        )
        
        if not content:
            return {
                "success": False,
                "error": f"Failed to extract meaningful content from {url}"
            }
        
        # Get metadata
        metadata = trafilatura.extract_metadata(downloaded)
        title = metadata.get('title', 'Untitled Page')
        
        return {
            "success": True,
            "content": content,
            "title": title,
            "url": url
        }
    except Exception as e:
        logger.error(f"Error parsing web page: {str(e)}")
        return {
            "success": False,
            "error": f"Error parsing web page: {str(e)}"
        }

async def parse_pdf(url: str) -> Dict[str, Any]:
    """
    Parse content from a PDF URL.
    
    Args:
        url: URL of the PDF to parse
        
    Returns:
        Result with parsed content
    """
    try:
        logger.debug(f"Fetching PDF content from: {url}")
        
        # Download the PDF to a temporary file
        async with httpx.AsyncClient() as client:
            response = await client.get(url, follow_redirects=True)
            response.raise_for_status()
            
            # Check if it's a PDF based on Content-Type
            content_type = response.headers.get('Content-Type', '')
            if 'application/pdf' not in content_type.lower():
                return {
                    "success": False,
                    "error": f"URL does not point to a PDF (Content-Type: {content_type})"
                }
            
            # Create a temporary file for the PDF
            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
                temp_path = temp_file.name
                temp_file.write(response.content)
        
        # Extract content using unstructured
        elements = partition_pdf(
            temp_path,
            strategy="hi_res",
            extract_images=False,
            extract_tables=True
        )
        
        # Convert to formatted text while preserving structure
        content = "\n\n".join([str(element) for element in elements])
        
        # Try to extract a title from the filename in the URL
        path_parts = url.split('/')
        filename = path_parts[-1].split('?')[0]  # Remove query parameters
        title = os.path.splitext(filename)[0].replace('-', ' ').replace('_', ' ').title()
        if not title:
            title = "PDF Document"
        
        # Clean up temporary file
        os.unlink(temp_path)
        
        return {
            "success": True,
            "content": content,
            "title": title,
            "url": url
        }
    except Exception as e:
        logger.error(f"Error parsing PDF: {str(e)}")
        # Clean up temporary file if it exists
        try:
            if 'temp_path' in locals():
                os.unlink(temp_path)
        except:
            pass
            
        return {
            "success": False,
            "error": f"Error parsing PDF: {str(e)}"
        }