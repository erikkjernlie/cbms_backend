import asyncio
import logging
import os
import struct
import aiokafka
import matplotlib.pyplot as plot
import scipy
import scipy.signal
import numpy as np
from aiohttp import web, WSMessage
from aiohttp_session import get_session
from src.clients.models import Client
from src.utils import RouteTableDefDocs, get_client, dumps

routes = RouteTableDefDocs()

logger = logging.getLogger(__name__)


@routes.get('/session')
async def session_endpoint(request: web.Request):
    """
    Only returns a session cookie

    Generates and returns a session cookie.
    """
    print("started a session")
    session = await get_session(request)
    session['id'] = client_id = request.remote  # TODO: better id

    if client_id not in request.app['clients']:
        print("HELLOOOo")
        print("HELLOOOo")
        print("HELLOOOo")
        logger.info('New connection from %s', request.remote)
        request.app['clients'][client_id] = Client()
    else:
        print("error")
        logger.info('Reconnection from %s', request.remote)

    return web.HTTPOk()


@routes.get('/')
async def index(request: web.Request):
    """
    The API index

    A standard HTTP request will return a sample page with a simple example of api use.
    A WebSocket request will initiate a websocket connection making it possible to retrieve measurement and simulation data.

    Available endpoints are
    - /client for information about the clients websocket connections
    - /datasources/ for measurement data sources
    - /processors/ for running processors on the data
    - /blueprints/ for the blueprints used to create processors
    - /fmus/ for available FMUs (for the fmu blueprint)
    - /models/ for available models (for the fedem blueprint)
    - /topics/ for all available data sources (datasources and processors)
    """
    print("before session")
    session = await get_session(request)
    ws = web.WebSocketResponse()  # TODO: fix heartbeat clientside?
    session['id'] = client_id = request.remote  # TODO: better id
    print("after session")
    if ws.can_prepare(request):
        await ws.prepare(request)
        if client_id not in request.app['clients']:
            print("HELLOOOo")
            print("HELLOOOo")
            print("HELLOOOo")
            logger.info('New connection from %s', request.remote)
            request.app['clients'][client_id] = Client()
        else:
            logger.info('Reconnection from %s', request.remote)
        client = request.app['clients'][client_id]
        await client.add_websocket_connection(ws)
        try:
            async for message in ws:  # type: WSMessage
                try:
                    if message.data == '__ping__':
                        await ws.send_bytes(b'')
                    # else:
                    #     print(message.json())
                except AttributeError as e:
                    logger.exception('Error receiving message from %s', client_id)
            return ws
        finally:
            logger.info('Closing %s', request.remote)
            await client.remove_websocket_connection(ws)
            await ws.close()
    else:
        with open('html/index.html', 'r') as file:
            body = file.read()
        return web.Response(body=body, content_type='text/html')

