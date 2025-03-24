from typing import Dict, List, Any, Optional, Union
import json
import httpx
import re
import os
import sys
from datetime import datetime
from mcp.server.fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("roam-helper")

# First, check for required environment variables and provide clear error messages if missing
API_TOKEN = os.environ.get("ROAM_API_TOKEN")
GRAPH_NAME = os.environ.get("ROAM_GRAPH_NAME")

# Validate environment variables
if not API_TOKEN or not GRAPH_NAME:
    missing_vars = []
    if not API_TOKEN:
        missing_vars.append("ROAM_API_TOKEN")
    if not GRAPH_NAME:
        missing_vars.append("ROAM_GRAPH_NAME")
    
    error_msg = (
        f"Missing required environment variables: {', '.join(missing_vars)}\n\n"
        "Please configure these variables in your Claude Desktop config:\n"
        "~/Library/Application Support/Claude/claude_desktop_config.json\n\n"
        'Example configuration:\n'
        '{\n'
        '  "mcpServers": {\n'
        '    "roam-helper": {\n'
        '      "command": "uvx",\n'
        '      "args": ["git+https://github.com/PhiloSolares/roam-mcp.git"],\n'
        '      "env": {\n'
        f'        "ROAM_API_TOKEN": "your-api-token",\n'
        f'        "ROAM_GRAPH_NAME": "{GRAPH_NAME or "your-graph-name"}"\n'
        '      }\n'
        '    }\n'
        '  }\n'
        '}'
    )
    print(error_msg, file=sys.stderr)
    # We'll continue execution but tools will return error messages

class RoamApiClient:
    """Client for interacting with the Roam Research API."""
    def __init__(self, api_token, graph_name, timeout=30.0):
        self.api_token = api_token
        self.graph_name = graph_name
        self.base_url = f"https://api.roamresearch.com/api/graph/{graph_name}"
        self.client = httpx.AsyncClient(verify=True, timeout=timeout)
    
    async def execute_query(self, query: str) -> Dict:
        """Execute a Roam Research Datalog query."""
        if not self.api_token or not self.graph_name:
            return {"error": "Missing API token or graph name"}
            
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        
        data = {"query": query}
        url = f"{self.base_url}/q"
        
        try:
            response = await self.client.post(url, json=data, headers=headers)
            print(f"Response status: {response.status_code}", file=sys.stderr)
            
            if response.status_code == 401:
                print("Authentication failed. Please check your API token.", file=sys.stderr)
                return {"error": "Authentication failed. Please check your API token."}
            
            if response.status_code != 200:
                print(f"Error: {response.status_code} - {response.text}", file=sys.stderr)
                return {"error": f"API request failed: {response.status_code} - {response.text}"}
            
            return response.json()
        except Exception as e:
            print(f"Error executing query: {e}", file=sys.stderr)
            return {"error": f"Error: {str(e)}"}
    
    async def execute_write(self, action: str, **kwargs) -> Dict:
        """Execute a Roam Research write operation."""
        if not self.api_token or not self.graph_name:
            return {"error": "Missing API token or graph name"}
            
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        
        data = {"action": action, **kwargs}
        url = f"{self.base_url}/write"
        
        try:
            response = await self.client.post(url, json=data, headers=headers)
            print(f"Response status: {response.status_code}", file=sys.stderr)
            
            if response.status_code == 401:
                print("Authentication failed. Please check your API token.", file=sys.stderr)
                return {"error": "Authentication failed. Please check your API token."}
            
            if response.status_code != 200:
                print(f"Error: {response.status_code} - {response.text}", file=sys.stderr)
                return {"error": f"API request failed: {response.status_code} - {response.text}"}
            
            return response.json()
        except Exception as e:
            print(f"Error executing write operation: {e}", file=sys.stderr)
            return {"error": f"Error: {str(e)}"}
    
    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()

def extract_youtube_video_id(url: str) -> Optional[str]:
    """Extract the video ID from a YouTube URL."""
    found = re.search(r"(?:youtu\.be\/|watch\?v=)([\w-]+)", url)
    if found:
        return found.group(1)
    return None

def process_results(raw_results):
    """Process raw search results to extract content, remove duplicates, and limit word count."""
    if isinstance(raw_results, dict) and "error" in raw_results:
        return [raw_results["error"]]
    
    unique_strings = set()
    processed_results = []
    word_count = 0
    max_word_count = 3000  # Maximum word count limit

    for block_list in raw_results:
        for block in block_list:
            block_string = block.get(':block/string')
            if block_string and block_string not in unique_strings:
                # Count the number of words in the block string
                block_word_count = len(block_string.split())

                # Check if adding this block string exceeds the word limit
                if word_count + block_word_count <= max_word_count:
                    unique_strings.add(block_string)
                    processed_results.append(block_string)
                    word_count += block_word_count
                else:
                    # Stop processing if the word limit is reached
                    return processed_results

    return processed_results

async def create_roam_client():
    """Create a Roam API client with credentials from environment variables."""
    return RoamApiClient(API_TOKEN, GRAPH_NAME)

@mcp.tool()
async def search_roam(search_terms: List[str]) -> str:
    """Search Roam database for content containing the specified terms.

    Args:
        search_terms: List of keywords to search for
    """
    if not API_TOKEN or not GRAPH_NAME:
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    client = await create_roam_client()
    try:
        all_results = []
        for keyword in search_terms:
            query = f'''[:find (pull ?b [*])
                         :where [?b :block/string ?s]
                                [(clojure.string/includes? ?s "{keyword}")]]'''

            response = await client.execute_query(query)
            if "error" in response:
                await client.close()
                return f"Error searching Roam: {response['error']}"
            
            all_results.extend(response.get('result', []))
        
        # Process results to extract content, remove duplicates, and limit word count
        processed_results = process_results(all_results)
        await client.close()
        
        return "\n\n".join(processed_results)
    except Exception as e:
        await client.close()
        return f"Error searching Roam: {str(e)}"

@mcp.tool()
async def create_page(page_title: str, content: List[Dict]) -> str:
    """Create a new page in Roam Research and link it in daily notes.

    Args:
        page_title: Title for the new page
        content: List of content blocks to add to the page
    """
    if not API_TOKEN or not GRAPH_NAME:
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    client = await create_roam_client()
    try:
        # Check if page exists
        query = f'''[:find ?uid
                     :where [?e :node/title "{page_title}"]
                            [?e :block/uid ?uid]]'''
        
        response = await client.execute_query(query)
        if "error" in response:
            await client.close()
            return f"Error checking if page exists: {response['error']}"
        
        result = response.get('result', [])
        if result:
            page_uid = result[0][0]
            print(f"Page exists with UID: {page_uid}", file=sys.stderr)
        else:
            # Create new page
            create_result = await client.execute_write("create-page", page={"title": page_title})
            if "error" in create_result:
                await client.close()
                return f"Error creating page: {create_result['error']}"
            
            page_uid = create_result.get("page", {}).get("uid")
            if not page_uid:
                await client.close()
                return "Failed to get UID for newly created page"
            
            print(f"Created new page with UID: {page_uid}", file=sys.stderr)
        
        # Add content blocks
        for i, block in enumerate(content):
            block_data = await client.execute_write(
                "create-block",
                location={
                    "parent-uid": page_uid,
                    "order": i
                },
                block={
                    "string": block.get("text", "")
                }
            )
            
            if "error" in block_data:
                await client.close()
                return f"Error adding content block: {block_data['error']}"
            
            # Handle children if present
            if "children" in block and block["children"]:
                # We need to find the UID of the newly created block
                child_query = f'''[:find ?uid
                                   :where [?b :block/string "{block.get('text', '')}"]
                                          [?b :block/uid ?uid]]'''
                
                child_response = await client.execute_query(child_query)
                if "error" not in child_response and child_response.get('result'):
                    parent_uid = child_response['result'][0][0]
                    
                    for j, child in enumerate(block["children"]):
                        child_data = await client.execute_write(
                            "create-block",
                            location={
                                "parent-uid": parent_uid,
                                "order": j
                            },
                            block={
                                "string": child.get("text", "")
                            }
                        )
                        
                        if "error" in child_data:
                            print(f"Error adding child block: {child_data['error']}", file=sys.stderr)
        
        # Try to link to today's daily notes
        today_date = datetime.now().strftime("%B %-dth, %Y")
        daily_query = f'''[:find ?uid
                           :where [?e :node/title "{today_date}"]
                                  [?e :block/uid ?uid]]'''
        
        daily_response = await client.execute_query(daily_query)
        if "error" not in daily_response and daily_response.get('result'):
            daily_uid = daily_response['result'][0][0]
            
            link_data = await client.execute_write(
                "create-block",
                location={
                    "parent-uid": daily_uid,
                    "order": 0
                },
                block={
                    "string": f"[[{page_title}]]"
                }
            )
            
            if "error" in link_data:
                print(f"Error linking to daily notes: {link_data['error']}", file=sys.stderr)
        
        # Return success message with link
        graph_link = f"https://roamresearch.com/#/app/{GRAPH_NAME}/page/{page_uid}"
        await client.close()
        return f"Content added to page and linked in Daily Notes: {graph_link}"
    
    except Exception as e:
        if client:
            await client.close()
        return f"Error creating page: {str(e)}"

@mcp.tool()
async def get_youtube_transcript(url: str) -> str:
    """Fetch and return the transcript of a YouTube video.

    Args:
        url: URL of the YouTube video
    """
    from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled

    video_id = extract_youtube_video_id(url)
    if not video_id:
        return "Invalid YouTube URL. Unable to extract video ID."

    try:
        # Define the prioritized list of language codes
        languages = [
            'en', 'en-US', 'en-GB', 'de', 'es', 'hi', 'zh', 'ar', 'bn', 'pt',
            'ru', 'ja', 'pa'
        ]

        # Attempt to retrieve the available transcripts
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)

        # Try to find a transcript in the prioritized languages
        for language in languages:
            try:
                transcript = transcript_list.find_transcript([language])
                # Check if the transcript is manually created or generated, prefer manually created
                if transcript.is_generated:
                    continue
                text = " ".join([line["text"] for line in transcript.fetch()])
                return text
            except Exception:
                continue

        # If no suitable transcript is found in the specified languages, try to fetch a generated transcript
        try:
            generated_transcript = transcript_list.find_generated_transcript(
                languages)
            text = " ".join(
                [line["text"] for line in generated_transcript.fetch()])
            return text
        except Exception:
            return "No suitable transcript found for this video."

    except TranscriptsDisabled:
        return "Transcripts are disabled for this video."
    except Exception as e:
        return f"An error occurred while fetching the transcript: {str(e)}"

@mcp.tool()
async def get_roam_graph_info() -> str:
    """
    Get information about a Roam Research graph.
    """
    if not API_TOKEN or not GRAPH_NAME:
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    client = await create_roam_client()
    try:
        # Get basic graph information
        graph_info_query = '''[:find (pull ?g [*])
                               :where [?g :graph/slug]]'''
        
        graph_info = await client.execute_query(graph_info_query)
        if "error" in graph_info:
            await client.close()
            return f"Error retrieving graph information: {graph_info['error']}"
        
        # Get page count
        page_count_query = '''[:find (count ?p)
                               :where [?p :node/title]]'''
        
        page_count = await client.execute_query(page_count_query)
        if "error" in page_count:
            await client.close()
            return f"Error retrieving page count: {page_count['error']}"
        
        # Format the output
        formatted_info = f"""
Graph Name: {GRAPH_NAME}
Pages: {page_count.get('result', [[0]])[0][0] if page_count.get('result') else 'Unknown'}
API Access: Enabled
        """
        
        await client.close()
        return formatted_info
    except Exception as e:
        if client:
            await client.close()
        return f"Error retrieving graph information: {str(e)}"

@mcp.prompt()
async def summarize_page(page_title: str) -> dict:
    """
    Create a prompt to summarize a page in Roam Research.

    Args:
        page_title: Title of the page to summarize
    """
    if not API_TOKEN or not GRAPH_NAME:
        return {
            "messages": [{
                "role": "user",
                "content": {
                    "type": "text",
                    "text": "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
                }
            }]
        }
    
    client = await create_roam_client()
    try:
        # Query to get the page content
        query = f'''[:find (pull ?b [:block/string])
                     :where [?p :node/title "{page_title}"]
                            [?b :block/page ?p]]'''
        
        response = await client.execute_query(query)
        if "error" in response:
            await client.close()
            return {
                "messages": [{
                    "role": "user",
                    "content": {
                        "type": "text",
                        "text": f"Error retrieving page content: {response['error']}"
                    }
                }]
            }
        
        page_blocks = [
            block[0].get(':block/string', '')
            for block in response.get('result', [])
        ]
        page_content = "\n".join(page_blocks)
        
        await client.close()
        return {
            "messages": [{
                "role": "user",
                "content": {
                    "type": "text",
                    "text": f"Please provide a concise summary of the following page content from my Roam Research database:\n\n{page_content}"
                }
            }]
        }
    except Exception as e:
        if client:
            await client.close()
        return {
            "messages": [{
                "role": "user",
                "content": {
                    "type": "text",
                    "text": f"I wanted to summarize my Roam page titled '{page_title}', but there was an error retrieving the content: {str(e)}. Can you help me troubleshoot this issue with my Roam Research integration?"
                }
            }]
        }

def run_server(transport="stdio", port=None):
    """Run the MCP server with the specified transport."""
    print("Server starting...", file=sys.stderr)
    
    # Print information about API token and graph name
    print(f"API token is {'set' if API_TOKEN else 'NOT SET'}", file=sys.stderr)
    print(f"Graph name is {'set' if GRAPH_NAME else 'NOT SET'}", file=sys.stderr)
    
    if API_TOKEN and GRAPH_NAME:
        print(f"API token length: {len(API_TOKEN)}", file=sys.stderr)
        print(f"Graph name: {GRAPH_NAME}", file=sys.stderr)
    else:
        print("WARNING: Missing required environment variables for Roam API", file=sys.stderr)
    
    # FastMCP.run() doesn't accept a port parameter, so we ignore it
    mcp.run(transport=transport)