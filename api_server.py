#!/usr/bin/env python3
"""
FastAPI REST API Server for Take5Algo1 Analytics

Provides HTTP endpoints to access sliding window buffer data with automatic Swagger documentation
"""

import logging
import threading
import asyncio
import signal
from datetime import datetime
from typing import Dict, List, Any, Optional
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import uvicorn
from asset_data.global_buffer_manager import global_buffer_manager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Pydantic models for API documentation
class AssetTrendRecord(BaseModel):
    """Single asset trend sliding window record"""
    asset: str = Field(..., description="Asset symbol (e.g., AAPL, GOOGL)")
    barTimeFrame: int = Field(..., description="Bar time frame in seconds")
    lastUpdateTime: Optional[int] = Field(None, description="Last update timestamp (milliseconds)")
    numRecords: int = Field(..., description="Number of records in sliding window")

class AssetTrendsResponse(BaseModel):
    """Response model for asset trends endpoint"""
    status: str = Field(..., description="Response status")
    timestamp: int = Field(..., description="Response timestamp")
    totalRecords: int = Field(..., description="Total number of records")
    data: List[AssetTrendRecord] = Field(..., description="Array of asset trend records")

class HealthResponse(BaseModel):
    """Health check response"""
    status: str = Field(..., description="Health status")
    service: str = Field(..., description="Service name")
    timestamp: int = Field(..., description="Current timestamp")

class StatsResponse(BaseModel):
    """System statistics response"""
    status: str = Field(..., description="Response status")
    timestamp: int = Field(..., description="Response timestamp")
    topics: List[str] = Field(..., description="List of active topics")
    totalTopics: int = Field(..., description="Total number of topics")
    totalCombinations: int = Field(..., description="Total asset/timeframe combinations")

class ErrorResponse(BaseModel):
    """Error response model"""
    status: str = Field(..., description="Error status")
    message: str = Field(..., description="Error message")
    timestamp: int = Field(..., description="Error timestamp")

class Take5ApiServer:
    """FastAPI server for Take5Algo1 analytics data"""
    
    def __init__(self, host='0.0.0.0', port=5000, take5_app=None):
        self.host = host
        self.port = port
        self.buffer_manager = global_buffer_manager
        self.take5_app = take5_app  # Reference to Take5Algo1 app for algorithm results
        self.logger = logging.getLogger(__name__)
        self.server = None
        self.server_thread = None
        self.shutdown_event = threading.Event()
        
        # Create FastAPI app with automatic documentation
        self.app = FastAPI(
            title="Take5Algo1 Analytics API",
            description="REST API for real-time financial analytics sliding window data",
            version="1.0.0",
            docs_url="/swagger",  # Swagger UI at /swagger
            redoc_url="/redoc",   # ReDoc at /redoc
        )
        
        # Register routes
        self._register_routes()
        
    def _register_routes(self):
        """Register all API routes with FastAPI"""
        
        @self.app.get(
            "/asset_trends",
            response_model=AssetTrendsResponse,
            summary="Get Asset Trends Data",
            description="Returns sliding window information for all assets and timeframes",
            tags=["Analytics"]
        )
        async def get_asset_trends():
            """
            Get all sliding window data for asset trends.
            
            Returns a JSON object containing:
            - asset: symbol name
            - barTimeFrame: time frame in seconds  
            - lastUpdateTime: timestamp of last update
            - numRecords: number of records in sliding window
            """
            try:
                # Dictionary to deduplicate and combine data from multiple topics
                combined_data = {}
                
                # Get all buffer statistics
                all_stats = self.buffer_manager.get_all_buffer_stats()
                
                for topic, assets in all_stats.items():
                    for asset, bar_time_frames in assets.items():
                        for bar_time_frame, buffer_info in bar_time_frames.items():
                            
                            # Create unique key for asset+barTimeFrame combination
                            key = (asset, bar_time_frame)
                            
                            if key not in combined_data:
                                # First occurrence - store the record
                                combined_data[key] = {
                                    'asset': asset,
                                    'barTimeFrame': bar_time_frame,
                                    'lastUpdateTime': buffer_info.latest_time,
                                    'numRecords': buffer_info.point_count,
                                    'topics': [topic]
                                }
                            else:
                                # Duplicate found - combine the data
                                existing = combined_data[key]
                                existing['topics'].append(topic)
                                
                                # Use the most recent lastUpdateTime
                                if (buffer_info.latest_time and 
                                    (not existing['lastUpdateTime'] or 
                                     buffer_info.latest_time > existing['lastUpdateTime'])):
                                    existing['lastUpdateTime'] = buffer_info.latest_time
                                
                                # Sum the record counts from all topics
                                existing['numRecords'] += buffer_info.point_count
                
                # Convert to list of AssetTrendRecord objects
                trends_data = []
                for data in combined_data.values():
                    record = AssetTrendRecord(
                        asset=data['asset'],
                        barTimeFrame=data['barTimeFrame'],
                        lastUpdateTime=data['lastUpdateTime'],
                        numRecords=data['numRecords']
                    )
                    trends_data.append(record)
                
                # Sort by asset, then by barTimeFrame for consistent ordering
                trends_data.sort(key=lambda x: (x.asset, x.barTimeFrame))
                
                return AssetTrendsResponse(
                    status="success",
                    timestamp=int(datetime.now().timestamp() * 1000),
                    totalRecords=len(trends_data),
                    data=trends_data
                )
                
            except Exception as e:
                self.logger.error(f"Error in /asset_trends endpoint: {e}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Internal server error: {str(e)}"
                )
        
        @self.app.get(
            "/health",
            response_model=HealthResponse,
            summary="Health Check",
            description="Check if the API service is running",
            tags=["System"]
        )
        async def health_check():
            """Health check endpoint"""
            return HealthResponse(
                status="healthy",
                service="Take5Algo1 API",
                timestamp=int(datetime.now().timestamp() * 1000)
            )
        
        @self.app.get(
            "/stats",
            response_model=StatsResponse,
            summary="System Statistics",
            description="Get overall system statistics including topics and combinations",
            tags=["System"]
        )
        async def get_stats():
            """Get overall system statistics"""
            try:
                all_topics = self.buffer_manager.get_all_topics()
                total_combinations = 0
                
                for topic in all_topics:
                    combinations = self.buffer_manager.get_all_combinations(topic)
                    total_combinations += len(combinations)
                
                return StatsResponse(
                    status="success",
                    timestamp=int(datetime.now().timestamp() * 1000),
                    topics=all_topics,
                    totalTopics=len(all_topics),
                    totalCombinations=total_combinations
                )
                
            except Exception as e:
                self.logger.error(f"Error in /stats endpoint: {e}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Internal server error: {str(e)}"
                )
        
        @self.app.get(
            "/asset_trends/{asset}",
            response_model=AssetTrendsResponse,
            summary="Get Asset Trends by Symbol",
            description="Get sliding window information for a specific asset across all timeframes",
            tags=["Analytics"]
        )
        async def get_asset_trends_by_symbol(asset: str):
            """
            Get sliding window data for a specific asset.
            
            Returns sliding windows for the specified asset across all timeframes and topics.
            """
            try:
                # Dictionary to deduplicate and combine data from multiple topics
                combined_data = {}
                
                # Get all buffer statistics
                all_stats = self.buffer_manager.get_all_buffer_stats()
                
                # Filter for the specific asset (case-insensitive)
                asset_upper = asset.upper()
                
                for topic, assets in all_stats.items():
                    for asset_symbol, bar_time_frames in assets.items():
                        if asset_symbol.upper() == asset_upper:
                            for bar_time_frame, buffer_info in bar_time_frames.items():
                                
                                # Create unique key for asset+barTimeFrame combination
                                key = (asset_symbol, bar_time_frame)
                                
                                if key not in combined_data:
                                    # First occurrence - store the record
                                    combined_data[key] = {
                                        'asset': asset_symbol,
                                        'barTimeFrame': bar_time_frame,
                                        'lastUpdateTime': buffer_info.latest_time,
                                        'numRecords': buffer_info.point_count,
                                        'topics': [topic]
                                    }
                                else:
                                    # Duplicate found - combine the data
                                    existing = combined_data[key]
                                    existing['topics'].append(topic)
                                    
                                    # Use the most recent lastUpdateTime
                                    if (buffer_info.latest_time and 
                                        (not existing['lastUpdateTime'] or 
                                         buffer_info.latest_time > existing['lastUpdateTime'])):
                                        existing['lastUpdateTime'] = buffer_info.latest_time
                                    
                                    # Sum the record counts from all topics
                                    existing['numRecords'] += buffer_info.point_count
                
                # Convert to list of AssetTrendRecord objects
                trends_data = []
                for data in combined_data.values():
                    record = AssetTrendRecord(
                        asset=data['asset'],
                        barTimeFrame=data['barTimeFrame'],
                        lastUpdateTime=data['lastUpdateTime'],
                        numRecords=data['numRecords']
                    )
                    trends_data.append(record)
                
                # Sort by barTimeFrame for consistent ordering
                trends_data.sort(key=lambda x: x.barTimeFrame)
                
                return AssetTrendsResponse(
                    status="success",
                    timestamp=int(datetime.now().timestamp() * 1000),
                    totalRecords=len(trends_data),
                    data=trends_data
                )
                
            except Exception as e:
                self.logger.error(f"Error in /asset_trends/{asset} endpoint: {e}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Internal server error: {str(e)}"
                )

        @self.app.get(
            "/algorithm-results",
            summary="Get All Algorithm Results",
            description="Get algorithm results for all assets",
            tags=["Algorithms"]
        )
        async def get_all_algorithm_results():
            """Get algorithm results for all assets"""
            try:
                if not self.take5_app:
                    raise HTTPException(
                        status_code=503,
                        detail="Algorithm results not available - Take5Algo1 app not connected"
                    )
                
                results = self.take5_app.get_algorithm_results()
                
                return {
                    'status': 'success',
                    'timestamp': int(datetime.now().timestamp() * 1000),
                    'totalAssets': len(results),
                    'data': results
                }
                
            except HTTPException:
                raise
            except Exception as e:
                self.logger.error(f"Error in /algorithm-results endpoint: {e}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Internal server error: {str(e)}"
                )

        @self.app.get(
            "/algorithm-results/{asset}",
            summary="Get Algorithm Results by Asset",
            description="Get algorithm results for a specific asset",
            tags=["Algorithms"]
        )
        async def get_algorithm_results_by_asset(asset: str):
            """Get algorithm results for a specific asset"""
            try:
                if not self.take5_app:
                    raise HTTPException(
                        status_code=503,
                        detail="Algorithm results not available - Take5Algo1 app not connected"
                    )
                
                asset_upper = asset.upper()
                results = self.take5_app.get_algorithm_results(asset_upper)
                
                if not results:
                    raise HTTPException(
                        status_code=404,
                        detail=f"No algorithm results found for asset {asset_upper}"
                    )
                
                return {
                    'status': 'success',
                    'timestamp': int(datetime.now().timestamp() * 1000),
                    'asset': asset_upper,
                    'data': results
                }
                
            except HTTPException:
                raise
            except Exception as e:
                self.logger.error(f"Error in /algorithm-results/{asset} endpoint: {e}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Internal server error: {str(e)}"
                )

        @self.app.get(
            "/asset/{asset_name}",
            summary="Get Asset Details",
            description="Get detailed information for a specific asset including recent messages",
            tags=["Analytics"]
        )
        async def get_asset_details(asset_name: str):
            """Get detailed information for a specific asset"""
            try:
                asset_data = []
                all_stats = self.buffer_manager.get_all_buffer_stats()
                
                for topic, assets in all_stats.items():
                    if asset_name.upper() in assets:
                        asset_info = assets[asset_name.upper()]
                        for bar_time_frame, buffer_info in asset_info.items():
                            # Get actual messages for this combination
                            messages = self.buffer_manager.get_messages(
                                topic, asset_name.upper(), bar_time_frame
                            )
                            
                            record = {
                                'topic': topic,
                                'asset': asset_name.upper(),
                                'barTimeFrame': bar_time_frame,
                                'lastUpdateTime': buffer_info.latest_time,
                                'lastUpdateTimeStr': buffer_info.latest_time_str,
                                'numRecords': buffer_info.point_count,
                                'windowSizeMs': buffer_info.window_size_ms,
                                'windowSpanMs': buffer_info.window_span_ms,
                                'oldestTime': buffer_info.oldest_time,
                                'oldestTimeStr': buffer_info.oldest_time_str,
                                'recentMessages': messages[-5:] if len(messages) > 5 else messages  # Last 5 messages
                            }
                            asset_data.append(record)
                
                if not asset_data:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Asset {asset_name} not found"
                    )
                
                return {
                    'status': 'success',
                    'timestamp': int(datetime.now().timestamp() * 1000),
                    'asset': asset_name.upper(),
                    'data': asset_data
                }
                
            except HTTPException:
                raise
            except Exception as e:
                self.logger.error(f"Error in /asset/{asset_name} endpoint: {e}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Internal server error: {str(e)}"
                )
    
    def run(self, debug=False):
        """Run the FastAPI server with Uvicorn"""
        self.logger.info(f"🌐 Starting Take5Algo1 FastAPI server on {self.host}:{self.port}")
        self.logger.info(f"📚 Swagger UI available at: http://{self.host}:{self.port}/swagger")
        self.logger.info(f"📖 ReDoc available at: http://{self.host}:{self.port}/redoc")
        
        # Create uvicorn config
        config = uvicorn.Config(
            self.app,
            host=self.host,
            port=self.port,
            log_level="info" if debug else "warning"
        )
        
        self.server = uvicorn.Server(config)
        
        # Run the server
        self.server.run()
    
    def run_in_thread(self, debug=False):
        """Run the FastAPI server in a separate thread with proper shutdown handling"""
        def run_server():
            try:
                # Create uvicorn config
                config = uvicorn.Config(
                    self.app,
                    host=self.host,
                    port=self.port,
                    log_level="info" if debug else "warning"
                )
                
                self.server = uvicorn.Server(config)
                
                # Run server until shutdown event is set
                asyncio.run(self._run_with_shutdown())
                
            except Exception as e:
                self.logger.error(f"Error in FastAPI server thread: {e}")
        
        self.server_thread = threading.Thread(target=run_server, daemon=True, name="FastApiServerThread")
        self.server_thread.start()
        self.logger.info(f"🌐 FastAPI server thread started on {self.host}:{self.port}")
        self.logger.info(f"📚 Swagger UI: http://{self.host}:{self.port}/swagger")
        self.logger.info(f"📖 ReDoc: http://{self.host}:{self.port}/redoc")
        return self.server_thread
    
    async def _run_with_shutdown(self):
        """Run server with shutdown event monitoring"""
        # Start the server
        server_task = asyncio.create_task(self.server.serve())
        
        # Monitor shutdown event in a separate task
        shutdown_task = asyncio.create_task(self._wait_for_shutdown())
        
        # Wait for either server to finish or shutdown event
        done, pending = await asyncio.wait(
            [server_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # Cancel remaining tasks
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    
    async def _wait_for_shutdown(self):
        """Wait for shutdown event in async context"""
        while not self.shutdown_event.is_set():
            await asyncio.sleep(0.1)
        
        # Shutdown the server
        if self.server:
            self.server.should_exit = True
            self.logger.info("🛑 FastAPI server shutdown initiated")
    
    def stop(self):
        """Stop the FastAPI server"""
        self.logger.info("🛑 Stopping FastAPI server...")
        self.shutdown_event.set()
        
        if self.server_thread and self.server_thread.is_alive():
            self.server_thread.join(timeout=5)
            if self.server_thread.is_alive():
                self.logger.warning("FastAPI server thread did not stop gracefully")
            else:
                self.logger.info("✅ FastAPI server stopped successfully")


def main():
    """Main function to run the API server standalone"""
    api_server = Take5ApiServer()
    api_server.run(debug=True)


if __name__ == "__main__":
    main() 