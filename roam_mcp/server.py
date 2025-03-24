from typing import Dict, List, Any, Optional, Union
import json
import httpx
import re
import os
import sys
import requests
from datetime import datetime
from mcp.server.fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("roam-helper")

# Get API token and graph name from environment variables
API_TOKEN = os.environ.get("ROAM_API_TOKEN")
GRAPH_NAME = os.environ.get("ROAM_GRAPH_NAME")

class PreserveAuthSession(requests.Session):
    """Session class that preserves authentication headers during redirects."""
    def rebuild_auth(self, prepared_request, response):
        # Preserve auth header during redirects
        return


def query_graph(api_token, graph_name, search_terms):
    """Query the Roam Research graph for blocks containing the specified search terms."""
    session = PreserveAuthSession()
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    endpoint = f'https://api.roamresearch.com/api/graph/{graph_name}/q'
    all_results = []

    # Iterate over the list of search terms
    for keyword in search_terms:
        query = f'''[:find (pull ?b [*])
                        :where [?b :block/string ?s]
                                [(clojure.string/includes? ?s "{keyword}")]]'''
        query = query.replace("\n", " ")
        data = {"query": query}

        # Make the API request
        r = session.post(url=endpoint, headers=headers, json=data)
        if r.status_code != 200:
            raise Exception(f"API request failed with status code {r.status_code}: {r.text}")

        # Add results to the collected list
        all_results.extend(r.json().get('result', []))

    return all_results


def find_block_uid(session, headers, graph_name, block_content):
    """Search for a block by its content to find its UID."""
    search_query = f'''[:find (pull ?e [:block/uid])
                      :where [?e :block/string "{block_content}"]]'''
    
    search_response = session.post(
        f'https://api.roamresearch.com/api/graph/{graph_name}/q',
        headers=headers,
        json={"query": search_query}
    )
    
    if search_response.status_code == 200 and search_response.json().get('result'):
        block_uid = search_response.json()['result'][0][0][':block/uid']
        return block_uid
    else:
        raise Exception("Failed to find the newly created block UID.")


def create_block(session, headers, graph_name, parent_uid, block_content, block_order):
    """Create a block and handle child blocks by finding the new block's UID."""
    block_data = {
        "action": "create-block",
        "location": {
            "parent-uid": parent_uid,
            "order": block_order
        },
        "block": {
            "string": block_content['text']
        }
    }
    
    block_resp = session.post(
        f'https://api.roamresearch.com/api/graph/{graph_name}/write',
        headers=headers,
        json=block_data
    )

    if block_resp.status_code != 200:
        raise Exception(f"Failed to add content to page: {block_resp.text}")

    # If the block has children, recursively handle them
    if 'children' in block_content:
        new_parent_uid = find_block_uid(session, headers, graph_name, block_content['text'])
        for order, child in enumerate(block_content['children']):
            create_block(session, headers, graph_name, new_parent_uid, child, order)


def create_page_and_link_in_daily_notes(api_token, graph_name, page_title, content):
    """Create a new page in Roam Research and link it in daily notes."""
    session = PreserveAuthSession()
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    # Check if the page exists and get its UID
    find_page_query = f'''[:find ?uid
                         :where [?e :node/title "{page_title}"]
                                [?e :block/uid ?uid]]'''
    
    find_page_resp = session.post(
        f'https://api.roamresearch.com/api/graph/{graph_name}/q',
        headers=headers,
        json={"query": find_page_query}
    )
    
    page_exists = find_page_resp.status_code == 200 and find_page_resp.json().get('result')
    page_uid = find_page_resp.json()['result'][0][0] if page_exists else None

    # Create a new page if it does not exist
    if not page_exists:
        create_page_data = {"action": "create-page", "page": {"title": page_title}}
        
        create_page_resp = session.post(
            f'https://api.roamresearch.com/api/graph/{graph_name}/write',
            headers=headers,
            json=create_page_data
        )
        
        if create_page_resp.status_code != 200:
            raise Exception(f"Failed to create page: {create_page_resp.text}")
        
        # Get UID of the newly created page
        page_uid = create_page_resp.json().get('page', {}).get('uid')

    # Add content to the page, supporting nested structures
    for block_order, block_content in enumerate(content):
        create_block(session, headers, graph_name, page_uid, block_content, block_order)

    # Attempt to link the new page in today's Daily Notes
    today_date = datetime.now().strftime("%B %-dth, %Y")
    daily_notes_query = f'''[:find ?uid
                             :where [?e :node/title "{today_date}"]
                                    [?e :block/uid ?uid]]'''
    
    daily_notes_resp = session.post(
        f'https://api.roamresearch.com/api/graph/{graph_name}/q',
        headers=headers,
        json={"query": daily_notes_query}
    )
    
    if daily_notes_resp.status_code == 200 and daily_notes_resp.json().get('result'):
        daily_notes_uid = daily_notes_resp.json()['result'][0][0]
        link_block_data = {
            "action": "create-block",
            "location": {
                "parent-uid": daily_notes_uid,
                "order": 0
            },
            "block": {
                "string": f"[[{page_title}]]"
            }
        }
        
        link_block_resp = session.post(
            f'https://api.roamresearch.com/api/graph/{graph_name}/write',
            headers=headers,
            json=link_block_data
        )
        
        if link_block_resp.status_code != 200:
            print(f"Failed to link page in Daily Notes: {link_block_resp.text}", file=sys.stderr)
    else:
        print("Daily Notes page for today not found or updated.", file=sys.stderr)

    # Return the Roam Research link to the page
    roam_page_link = f"https://roamresearch.com/#/app/{graph_name}/page/{page_uid}"
    return f"Content added to page and linked in Daily Notes: {roam_page_link}"


def extract_youtube_video_id(url: str) -> Optional[str]:
    """Extract the video ID from a YouTube URL."""
    found = re.search(r"(?:youtu\.be\/|watch\?v=)([\w-]+)", url)
    if found:
        return found.group(1)
    return None


def process_results(raw_results):
    """Process raw search results to extract content, remove duplicates, and limit word count."""
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


@mcp.tool()
async def search_roam(search_terms: List[str]) -> str:
    """Search Roam database for content containing the specified terms.

    Args:
        search_terms: List of keywords to search for
    """
    if not API_TOKEN or not GRAPH_NAME:
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        # Use the original queryGraph function with synchronous requests
        results = query_graph(API_TOKEN, GRAPH_NAME, search_terms)
        processed_results = process_results(results)
        
        return "\n\n".join(processed_results)
    except Exception as e:
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
    
    try:
        # Use the original create_page_and_link_in_daily_notes function
        result = create_page_and_link_in_daily_notes(API_TOKEN, GRAPH_NAME, page_title, content)
        return result
    except Exception as e:
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
    
    try:
        session = PreserveAuthSession()
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {API_TOKEN}",
            "Content-Type": "application/json",
        }
        
        # Get basic graph information
        graph_info_query = '''[:find (pull ?g [*])
                               :where [?g :graph/slug]]'''
        
        graph_info_resp = session.post(
            f'https://api.roamresearch.com/api/graph/{GRAPH_NAME}/q',
            headers=headers,
            json={"query": graph_info_query}
        )
        
        if graph_info_resp.status_code != 200:
            return f"Error: Failed to retrieve graph information: {graph_info_resp.text}"
        
        # Get page count
        page_count_query = '''[:find (count ?p)
                               :where [?p :node/title]]'''
        
        page_count_resp = session.post(
            f'https://api.roamresearch.com/api/graph/{GRAPH_NAME}/q',
            headers=headers,
            json={"query": page_count_query}
        )
        
        if page_count_resp.status_code != 200:
            return f"Error: Failed to retrieve page count: {page_count_resp.text}"
        
        page_count = page_count_resp.json().get('result', [[0]])[0][0]
        
        # Format the output
        formatted_info = f"""
Graph Name: {GRAPH_NAME}
Pages: {page_count}
API Access: Enabled
        """
        
        return formatted_info
    except Exception as e:
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
    
    try:
        session = PreserveAuthSession()
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {API_TOKEN}",
            "Content-Type": "application/json",
        }
        
        # Query to get the page content
        query = f'''[:find (pull ?b [:block/string])
                     :where [?p :node/title "{page_title}"]
                            [?b :block/page ?p]]'''
        
        response = session.post(
            f'https://api.roamresearch.com/api/graph/{GRAPH_NAME}/q',
            headers=headers,
            json={"query": query}
        )
        
        if response.status_code != 200:
            return {
                "messages": [{
                    "role": "user",
                    "content": {
                        "type": "text",
                        "text": f"Error retrieving page content: {response.text}"
                    }
                }]
            }
        
        page_blocks = [
            block[0].get(':block/string', '')
            for block in response.json().get('result', [])
        ]
        page_content = "\n".join(page_blocks)
        
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