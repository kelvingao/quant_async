"""
Production monitoring and health checks for Quant Async.

This module provides:
- Health check endpoints for system status
- Metrics collection for data flow rates  
- Connection status monitoring for IB and database
- Performance monitoring and alerting
- Integration with existing logging framework
"""
import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from collections import defaultdict, deque

import asyncpg


@dataclass 
class HealthStatus:
    """Health status for a system component."""
    component: str
    status: str  # "healthy", "degraded", "unhealthy"
    message: str
    last_check: datetime
    metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SystemMetrics:
    """System-wide metrics."""
    startup_time: datetime
    message_count: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    message_rates: Dict[str, float] = field(default_factory=dict)
    error_counts: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    connection_status: Dict[str, bool] = field(default_factory=dict)
    last_activity: Dict[str, datetime] = field(default_factory=dict)
    performance_metrics: Dict[str, Any] = field(default_factory=dict)


class HealthChecker:
    """Health check manager for system components."""
    
    def __init__(self):
        self.checks: Dict[str, HealthStatus] = {}
        self.logger = logging.getLogger("quant_async.health")
        
    def register_component(self, component: str, initial_status: str = "unknown", 
                          message: str = "Not checked"):
        """Register a component for health monitoring."""
        self.checks[component] = HealthStatus(
            component=component,
            status=initial_status,
            message=message,
            last_check=datetime.now()
        )
        self.logger.info(f"Registered health check for {component}")
    
    def update_status(self, component: str, status: str, message: str, 
                     metrics: Optional[Dict[str, Any]] = None):
        """Update component health status."""
        if component not in self.checks:
            self.register_component(component)
        
        self.checks[component].status = status
        self.checks[component].message = message
        self.checks[component].last_check = datetime.now()
        
        if metrics:
            self.checks[component].metrics.update(metrics)
        
        if status != "healthy":
            self.logger.warning(f"Health check {component}: {status} - {message}")
    
    def get_overall_status(self) -> Dict[str, Any]:
        """Get overall system health status."""
        if not self.checks:
            return {"status": "unknown", "message": "No health checks registered"}
        
        unhealthy_count = sum(1 for check in self.checks.values() if check.status == "unhealthy")
        degraded_count = sum(1 for check in self.checks.values() if check.status == "degraded")
        
        if unhealthy_count > 0:
            overall_status = "unhealthy"
            message = f"{unhealthy_count} component(s) unhealthy"
        elif degraded_count > 0:
            overall_status = "degraded"
            message = f"{degraded_count} component(s) degraded"
        else:
            overall_status = "healthy"
            message = "All components healthy"
        
        return {
            "status": overall_status,
            "message": message,
            "components": {
                name: {
                    "status": check.status,
                    "message": check.message,
                    "last_check": check.last_check.isoformat(),
                    "metrics": check.metrics
                }
                for name, check in self.checks.items()
            },
            "summary": {
                "total_components": len(self.checks),
                "healthy": sum(1 for c in self.checks.values() if c.status == "healthy"),
                "degraded": degraded_count,
                "unhealthy": unhealthy_count
            }
        }


class MetricsCollector:
    """Metrics collection and aggregation."""
    
    def __init__(self, window_size: int = 100):
        self.metrics = SystemMetrics(startup_time=datetime.now())
        self.window_size = window_size
        self.message_timestamps: Dict[str, deque] = defaultdict(lambda: deque(maxlen=window_size))
        self.logger = logging.getLogger("quant_async.metrics")
        
    def record_message(self, message_type: str, symbol: Optional[str] = None):
        """Record a message for metrics."""
        now = datetime.now()
        key = f"{message_type}_{symbol}" if symbol else message_type
        
        self.metrics.message_count[key] += 1
        self.message_timestamps[key].append(now)
        self.metrics.last_activity[key] = now
        
        # Calculate message rate (messages per second over window)
        timestamps = self.message_timestamps[key]
        if len(timestamps) >= 2:
            time_span = (timestamps[-1] - timestamps[0]).total_seconds()
            if time_span > 0:
                self.metrics.message_rates[key] = len(timestamps) / time_span
    
    def record_error(self, error_type: str, details: Optional[str] = None):
        """Record an error for metrics."""
        self.metrics.error_counts[error_type] += 1
        self.logger.warning(f"Error recorded: {error_type} - {details}")
    
    def update_connection_status(self, connection: str, status: bool):
        """Update connection status."""
        old_status = self.metrics.connection_status.get(connection)
        self.metrics.connection_status[connection] = status
        
        if old_status is not None and old_status != status:
            status_str = "connected" if status else "disconnected"
            self.logger.info(f"Connection {connection}: {status_str}")
    
    def record_performance_metric(self, metric_name: str, value: Any):
        """Record a performance metric."""
        self.metrics.performance_metrics[metric_name] = {
            "value": value,
            "timestamp": datetime.now().isoformat()
        }
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get metrics summary."""
        uptime = (datetime.now() - self.metrics.startup_time).total_seconds()
        
        # Calculate overall message rate
        total_messages = sum(self.metrics.message_count.values())
        overall_rate = total_messages / uptime if uptime > 0 else 0
        
        return {
            "uptime_seconds": uptime,
            "startup_time": self.metrics.startup_time.isoformat(),
            "message_counts": dict(self.metrics.message_count),
            "message_rates": dict(self.metrics.message_rates),
            "overall_message_rate": overall_rate,
            "error_counts": dict(self.metrics.error_counts),
            "connection_status": dict(self.metrics.connection_status),
            "last_activity": {
                k: v.isoformat() for k, v in self.metrics.last_activity.items()
            },
            "performance_metrics": dict(self.metrics.performance_metrics)
        }


class ConnectionMonitor:
    """Monitor connection health for various components."""
    
    def __init__(self, health_checker: HealthChecker, metrics_collector: MetricsCollector):
        self.health_checker = health_checker
        self.metrics_collector = metrics_collector
        self.logger = logging.getLogger("quant_async.connection_monitor")
        
        # Register components
        self.health_checker.register_component("interactive_brokers", "unknown", "Not connected")
        self.health_checker.register_component("database", "unknown", "Not connected")
        self.health_checker.register_component("zeromq", "unknown", "Not initialized")
    
    async def check_ib_connection(self, ezib) -> bool:
        """Check Interactive Brokers connection."""
        try:
            if hasattr(ezib, 'connected') and ezib.connected:
                self.health_checker.update_status(
                    "interactive_brokers", 
                    "healthy", 
                    "Connected to IB Gateway/TWS",
                    {"connected": True, "client_id": getattr(ezib, 'client', {}).get('clientId', 'unknown')}
                )
                self.metrics_collector.update_connection_status("ib", True)
                return True
            else:
                self.health_checker.update_status(
                    "interactive_brokers",
                    "unhealthy", 
                    "Not connected to IB Gateway/TWS"
                )
                self.metrics_collector.update_connection_status("ib", False)
                return False
                
        except Exception as e:
            self.health_checker.update_status(
                "interactive_brokers",
                "unhealthy",
                f"Error checking IB connection: {str(e)}"
            )
            self.metrics_collector.record_error("ib_connection_check", str(e))
            return False
    
    async def check_database_connection(self, pool: Optional[asyncpg.Pool]) -> bool:
        """Check database connection."""
        if pool is None:
            self.health_checker.update_status(
                "database",
                "degraded",
                "Database connection disabled (dbskip=True)"
            )
            return True  # Not an error if intentionally skipped
        
        try:
            async with pool.acquire() as conn:
                # Simple connectivity test
                result = await conn.fetchval("SELECT 1")
                if result == 1:
                    pool_stats = {
                        "size": pool.get_size(),
                        "idle": pool.get_idle_size(),
                        "min_size": pool.get_min_size(),
                        "max_size": pool.get_max_size()
                    }
                    
                    self.health_checker.update_status(
                        "database",
                        "healthy",
                        "Database connection healthy",
                        pool_stats
                    )
                    self.metrics_collector.update_connection_status("database", True)
                    return True
                    
        except Exception as e:
            self.health_checker.update_status(
                "database",
                "unhealthy",
                f"Database connection failed: {str(e)}"
            )
            self.metrics_collector.record_error("database_connection_check", str(e))
            self.metrics_collector.update_connection_status("database", False)
            return False
        
        return False
    
    async def check_zeromq_socket(self, socket) -> bool:
        """Check ZeroMQ socket health."""
        if socket is None:
            self.health_checker.update_status(
                "zeromq",
                "unhealthy",
                "ZeroMQ socket not initialized"
            )
            return False
        
        try:
            # Check if socket is still alive
            transport = getattr(socket, 'transport', None)
            if transport and hasattr(transport, 'bindings'):
                bindings = transport.bindings()
                
                self.health_checker.update_status(
                    "zeromq",
                    "healthy",
                    "ZeroMQ socket healthy",
                    {"bindings": bindings}
                )
                self.metrics_collector.update_connection_status("zeromq", True)
                return True
            else:
                self.health_checker.update_status(
                    "zeromq",
                    "degraded",
                    "ZeroMQ socket transport not available"
                )
                return False
                
        except Exception as e:
            self.health_checker.update_status(
                "zeromq",
                "unhealthy",
                f"ZeroMQ socket check failed: {str(e)}"
            )
            self.metrics_collector.record_error("zeromq_socket_check", str(e))
            self.metrics_collector.update_connection_status("zeromq", False)
            return False


class PerformanceMonitor:
    """Monitor system performance metrics."""
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics_collector = metrics_collector
        self.logger = logging.getLogger("quant_async.performance")
        
        # Performance tracking
        self.operation_times: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
    
    def time_operation(self, operation_name: str):
        """Context manager to time operations."""
        return OperationTimer(self, operation_name)
    
    def record_operation_time(self, operation_name: str, duration: float):
        """Record operation timing."""
        self.operation_times[operation_name].append(duration)
        
        # Calculate average over recent operations
        times = list(self.operation_times[operation_name])
        if times:
            avg_time = sum(times) / len(times)
            max_time = max(times)
            min_time = min(times)
            
            self.metrics_collector.record_performance_metric(
                f"{operation_name}_timing",
                {
                    "avg_ms": avg_time * 1000,
                    "max_ms": max_time * 1000,
                    "min_ms": min_time * 1000,
                    "sample_count": len(times)
                }
            )
            
            # Alert on slow operations
            if avg_time > 1.0:  # More than 1 second average
                self.logger.warning(f"Slow operation {operation_name}: {avg_time:.3f}s average")


class OperationTimer:
    """Context manager for timing operations."""
    
    def __init__(self, performance_monitor: PerformanceMonitor, operation_name: str):
        self.performance_monitor = performance_monitor
        self.operation_name = operation_name
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = time.time() - self.start_time
            self.performance_monitor.record_operation_time(self.operation_name, duration)


class SystemMonitor:
    """Main system monitoring coordinator."""
    
    def __init__(self):
        self.health_checker = HealthChecker()
        self.metrics_collector = MetricsCollector()
        self.connection_monitor = ConnectionMonitor(self.health_checker, self.metrics_collector)
        self.performance_monitor = PerformanceMonitor(self.metrics_collector)
        self.logger = logging.getLogger("quant_async.system_monitor")
        
        # Monitoring tasks
        self.monitoring_tasks: List[asyncio.Task] = []
        self.monitoring_interval = 30  # seconds
        self.running = False
    
    async def start_monitoring(self, blotter=None):
        """Start background monitoring tasks."""
        if self.running:
            return
            
        self.running = True
        self.logger.info("Starting system monitoring")
        
        # Start periodic health checks
        if blotter:
            task = asyncio.create_task(self._periodic_health_checks(blotter))
            self.monitoring_tasks.append(task)
        
        # Start metrics reporting
        task = asyncio.create_task(self._periodic_metrics_reporting())
        self.monitoring_tasks.append(task)
    
    async def stop_monitoring(self):
        """Stop monitoring tasks."""
        self.running = False
        self.logger.info("Stopping system monitoring")
        
        for task in self.monitoring_tasks:
            if not task.done():
                task.cancel()
                
        if self.monitoring_tasks:
            await asyncio.gather(*self.monitoring_tasks, return_exceptions=True)
        
        self.monitoring_tasks.clear()
    
    async def _periodic_health_checks(self, blotter):
        """Periodic health check loop."""
        while self.running:
            try:
                # Check IB connection
                if hasattr(blotter, 'ezib'):
                    await self.connection_monitor.check_ib_connection(blotter.ezib)
                
                # Check database connection
                if hasattr(blotter, 'pool'):
                    await self.connection_monitor.check_database_connection(blotter.pool)
                
                # Check ZeroMQ socket
                if hasattr(blotter, 'socket'):
                    await self.connection_monitor.check_zeromq_socket(blotter.socket)
                
                await asyncio.sleep(self.monitoring_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in periodic health checks: {e}")
                await asyncio.sleep(self.monitoring_interval)
    
    async def _periodic_metrics_reporting(self):
        """Periodic metrics reporting loop."""
        while self.running:
            try:
                # Log metrics summary periodically
                metrics = self.metrics_collector.get_metrics_summary()
                
                # Log key metrics
                if metrics["overall_message_rate"] > 0:
                    self.logger.info(f"System metrics - Messages/sec: {metrics['overall_message_rate']:.2f}, "
                                   f"Uptime: {metrics['uptime_seconds']:.0f}s, "
                                   f"Total messages: {sum(metrics['message_counts'].values())}")
                
                await asyncio.sleep(self.monitoring_interval * 2)  # Less frequent than health checks
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in periodic metrics reporting: {e}")
                await asyncio.sleep(self.monitoring_interval * 2)
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status."""
        health_status = self.health_checker.get_overall_status()
        metrics_summary = self.metrics_collector.get_metrics_summary()
        
        return {
            "timestamp": datetime.now().isoformat(),
            "health": health_status,
            "metrics": metrics_summary,
            "monitoring": {
                "running": self.running,
                "interval_seconds": self.monitoring_interval,
                "active_tasks": len([t for t in self.monitoring_tasks if not t.done()])
            }
        }
    
    # Convenience methods for external use
    def record_message(self, message_type: str, symbol: Optional[str] = None):
        """Record a message."""
        self.metrics_collector.record_message(message_type, symbol)
    
    def record_error(self, error_type: str, details: Optional[str] = None):
        """Record an error."""
        self.metrics_collector.record_error(error_type, details)
    
    def time_operation(self, operation_name: str):
        """Time an operation."""
        return self.performance_monitor.time_operation(operation_name)