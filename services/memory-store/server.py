"""Memory Store Service - Temporary queue for raw items from scrapers"""
import asyncio
import os
from aiohttp import web
import json

headers = {'Access-Control-Allow-Origin': '*'}

class MemoryServer:
    def __init__(self, address="0.0.0.0", port=6379):
        # Raw items queue - consumed on read by ingestion service
        # Scrapers push raw unprocessed data here
        self.raw_items = []
        
        # Cluster queue - consumed on read by clustering service
        # Ingestion service pushes items here for clustering
        self.cluster_queue = []
        
        # Track seen item IDs to avoid duplicates in queue
        self.seen_ids = set()
        
        # Persistent configuration stores (NOT consumed on read)
        # Frontend sends these, scrapers read them
        self.tweet_sources = {
            "home_timeline": True,
            "search": True,
            "home_latest_timeline": True
        }
        self.search_queries = ["breaking"]
        
        self.address = address
        self.port = port
        self.runner = None
        self.site = None

    async def handle_get(self, request):
        key = request.match_info.get('key')
        
        # Raw items queue - consumed on read by ingestion service
        if key == 'raw_items':
            value = self.raw_items
            self.raw_items = []  # Consume the queue
            return web.json_response({"raw_items": value}, headers=headers)
        
        # Persistent configuration stores (NOT consumed on read)
        if key == 'tweet_sources':
            return web.json_response({"tweet_sources": self.tweet_sources}, headers=headers)
        
        if key == 'search_queries':
            return web.json_response({"search_queries": self.search_queries}, headers=headers)
        
        # Health check
        if key == 'health':
            return web.json_response({
                "status": "healthy", 
                "raw_items_queue_size": len(self.raw_items)
            }, headers=headers)
        
        return web.Response(status=404, text="Key not found", headers=headers)

    async def handle_post(self, request):
        value_dict = await request.json()
        key, value = value_dict.get("key"), value_dict.get("value")
        
        # Raw items queue - scrapers push unprocessed data here
        if key == "raw_items":
            added = 0
            for item in value:
                self.raw_items.append(item)
                added += 1
            return web.json_response({
                'status': 'success', 
                'added': added, 
                'queue_size': len(self.raw_items)
            }, headers=headers)
                
        # Persistent configuration stores (NOT consumed on read)
        if key == "tweet_sources":
            if isinstance(value, dict):
                self.tweet_sources = value
                return web.json_response({
                    'status': 'success',
                    'tweet_sources': self.tweet_sources
                }, headers=headers)
            else:
                return web.json_response({
                    'status': 'error',
                    'message': 'tweet_sources must be a dictionary'
                }, status=400, headers=headers)
        
        if key == "search_queries":
            if isinstance(value, list):
                self.search_queries = value
                return web.json_response({
                    'status': 'success',
                    'search_queries': self.search_queries
                }, headers=headers)
            else:
                return web.json_response({
                    'status': 'error',
                    'message': 'search_queries must be a list'
                }, status=400, headers=headers)
        
        return web.json_response({'status': 'error', 'message': 'Unknown key'}, status=400, headers=headers)
    
    async def handle_options(self, request):
        # Handle CORS preflight requests
        return web.Response(
            status=200,
            headers={
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Max-Age': '3600'
            }
        )

    async def start_server(self):
        print(f"Starting memory store server on http://{self.address}:{self.port}")
        app = web.Application(client_max_size=100000000)  # 100MB limit
        app.add_routes([web.get('/get/{key}', self.handle_get)])
        app.add_routes([web.post('/post', self.handle_post)])
        app.add_routes([web.options('/post', self.handle_options)])
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.address, self.port)
        await self.site.start()

    async def stop_server(self):
        if self.runner:
            await self.runner.cleanup()

async def main():
    """Main entry point"""
    address = os.getenv('MEMORY_STORE_ADDRESS', '0.0.0.0')
    port = int(os.getenv('MEMORY_STORE_PORT', '6379'))
    
    memory_server = MemoryServer(address=address, port=port)
    await memory_server.start_server()
    
    try:
        await asyncio.Event().wait()  # Run forever
    except KeyboardInterrupt:
        pass
    finally:
        await memory_server.stop_server()

if __name__ == '__main__':
    asyncio.run(main())
