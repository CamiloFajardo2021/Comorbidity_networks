"""
Shared dependency for routers to reach the database.

main.py opens the Motor client once in its lifespan handler and stores it
on app.state; routers pull it out via this dependency rather than each
opening their own connection.
"""

from fastapi import Request
from motor.motor_asyncio import AsyncIOMotorDatabase


def get_database(request: Request) -> AsyncIOMotorDatabase:
    return request.app.state.db
