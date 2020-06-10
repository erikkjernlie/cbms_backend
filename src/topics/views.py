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


@routes.get('/topics/', name='topics')
async def topics(request: web.Request):
    """
    Lists the available data sources for plotting or processors

    Append the id of a topic to get details about only that topic
    Append the id of a topic and /subscribe to subscribe to a topic
    Append the id of a topic and /unsubscribe to unsubscribe to a topic
    Append the id of a topic and /history to get historic data from a topic
    """

    return web.json_response(request.app['topics'])


@routes.get('/topics/{id}', name='topics_detail')
async def topics_detail(request: web.Request):
    """
    Show a single topic

    Append /subscribe to subscribe to the topic
    Append /unsubscribe to unsubscribe to the topic
    Append /history to get historic data from a topic
    """
    topic = request.match_info['id']
    if topic not in request.app['topics']:
        raise web.HTTPNotFound()
    return web.json_response(request.app['topics'][topic])


@routes.get('/topics/{id}/subscribe', name='subscribe')
async def subscribe(request: web.Request):
    """Subscribe to the given topic"""
    topic = request.match_info['id']
    client = await get_client(request)
    if topic not in request.app['topics']:
        raise web.HTTPNotFound()
    request.app['subscribers'][topic].add(client)
    print("client: ", client, "added to topic ", topic)
    raise web.HTTPAccepted()


@routes.get('/topics/{id}/unsubscribe', name='unsubscribe')
async def unsubscribe(request: web.Request):
    """Unsubscribe to the given topic"""
    topic = request.match_info['id']
    client = await get_client(request)
    if topic not in request.app['topics']:
        raise web.HTTPNotFound()
    request.app['subscribers'][topic].discard(client)
    raise web.HTTPAccepted()


@routes.get('/fft/{id}/{channel_id}/{start}/{end}/{sample_spacing}', name='fft_from_source')
async def fft_from_source(request: web.Request):
    datasource_id = request.match_info['id']
    datasource = request.app['datasources'].get_source(datasource_id)
    input_byte_format = datasource.input_byte_format
    metadata = len(datasource.input_names) - len(datasource.output_names)
    topic = datasource.topic
    channel_id = int(request.match_info['channel_id'])
    start = int(request.match_info['start'])
    end = int(request.match_info['end'])
    sample_spacing = float(request.match_info['sample_spacing'])
    print(isinstance(start, int), isinstance(end, int))
    if not isinstance(start, int) or not isinstance(end, int):
        print("bad request")
        raise web.HTTPBadRequest()
    if topic not in request.app['topics']:
        print("no topic found")
        raise web.HTTPNotFound()

    consumer = aiokafka.AIOKafkaConsumer(
        topic,
        loop=asyncio.get_event_loop(),
        api_version='2.2.0',
        bootstrap_servers=request.app['settings'].KAFKA_SERVER
    )
    await consumer.start()
    topic_partition = aiokafka.TopicPartition(topic, 0)
    offsets = await consumer.offsets_for_times({topic_partition: start})
    if offsets is None:
        print("offsets is none")
        raise web.HTTPNotFound()
    offset = offsets[topic_partition].offset
    consumer.seek(topic_partition, offset)
    data = []
    try:
        async for msg in consumer:
            if msg.timestamp > end:
                break
            for i, message in enumerate(struct.iter_unpack(input_byte_format, msg.value)):
                # print("i and value", i, message)
                # print("value: ", message[metadata + channel_id])
                data.append(message[metadata + channel_id])
    finally:
        await consumer.stop()
        print("data: ", data)
        if len(data) == 0:
            print("no data (len == 0)")
            raise web.HTTPBadRequest()
        resp = get_fft(data, sample_spacing)
        print("resp", resp)
        return web.json_response(resp, dumps=dumps)


def get_fft(data, sample_spacing):
    fft_values = scipy.fftpack.fft(data)

    if isinstance(data, list):
        window = len(data)
    else:
        window = data.shape[-1]
    print("window:", window)
    x = np.fft.rfftfreq(int(window), d=sample_spacing)
    y = 2.0 / window * np.abs(fft_values[:window // 2])
    return [[val for val in x], [float(val) for val in y]]


@routes.post('/topics/{id}/spectrogram', name='spectrogram')
async def spectrogram(request: web.Request):
    """
    Create spectogram from data from the given topic

    post params:
    - start: the start timestamp as milliseconds since 00:00:00 Thursday, 1 January 1970
    - end: (optional) the end timestamp as milliseconds since 00:00:00 Thursday, 1 January 1970
    - channel: Id of channel in topic that should make the plot
    - projectId: Id of the project where plot should be saved
    """
    post = await request.post()
    datasource_id = request.match_info['id']
    datasource = request.app['datasources'].get_source(datasource_id)
    input_byte_format = datasource.input_byte_format
    topic = datasource.topic

    channel_id = int(post.get('channel_id'))
    start = int(post.get('start'))
    end = int(post.get('end'))

    if not isinstance(start, int) or not isinstance(end, int):
        print("bad request")
        raise web.HTTPBadRequest()
    if topic not in request.app['topics']:
        print("no topic found")
        raise web.HTTPNotFound()

    consumer = aiokafka.AIOKafkaConsumer(
        topic,
        loop=asyncio.get_event_loop(),
        api_version='2.2.0',
        bootstrap_servers=request.app['settings'].KAFKA_SERVER
    )
    await consumer.start()
    topic_partition = aiokafka.TopicPartition(topic, 0)
    offsets = await consumer.offsets_for_times({topic_partition: start})
    if offsets is None:
        raise web.HTTPNotFound()
    offset = offsets[topic_partition].offset
    consumer.seek(topic_partition, offset)
    data = []
    try:
        async for msg in consumer:
            if msg.timestamp > end:
                break
            for i, message in enumerate(struct.iter_unpack(input_byte_format, msg.value)):
                data.append(message[1 + channel_id])
    finally:
        await consumer.stop()
        if len(data) == 0:
            raise web.HTTPBadRequest()
        resp = make_spectrogram(data, (end - start) / 1000)
        return web.json_response(resp, dumps=dumps)


def make_spectrogram(frequencies, duration):
    sampling_frequency = len(frequencies) / duration
    print("sampling freq", sampling_frequency)
    print("frequency", frequencies)
    fig, ax = plot.subplots(1)
    power_spectrum, frequencies_found, time, image_axis = ax.specgram([f for f in frequencies], Fs=sampling_frequency)
    # f, t, Sxx = scipy.signal.spectrogram(frequencies, sampling_frequency)
    data = [
        [[x for x in row] for row in power_spectrum],
        [freq for freq in frequencies_found],
        [t for t in time]
    ]
    # plot.show()

    return data


@routes.get('/topics/{id}/history', name='history')
async def history(request: web.Request):
    """
    Get historic data from the given topic

    get params:
    - start: the start timestamp as milliseconds since 00:00:00 Thursday, 1 January 1970
    - end: (optional) the end timestamp as milliseconds since 00:00:00 Thursday, 1 January 1970
    """

    print("history")
    topic = request.match_info['id']
    print("topic", topic)
    start = int(request.query['start'])
    print("start param", start, isinstance(start, int))

    end = int(request.query['end']) if 'end' in request.query else 9999999999999
    print(start, end)
    if not isinstance(start, int) or not isinstance(end, int):
        print("bad request")
        raise web.HTTPBadRequest()
    if topic not in request.app['topics']:
        print("not found topic")
        raise web.HTTPNotFound()
    consumer = aiokafka.AIOKafkaConsumer(
        topic,
        loop=asyncio.get_event_loop(),
        api_version='2.2.0',
        bootstrap_servers=request.app['settings'].KAFKA_SERVER
    )
    print("starting consumer")
    await consumer.start()
    topic_partition = aiokafka.TopicPartition(topic, 0)
    offsets = await consumer.offsets_for_times({topic_partition: start})
    if offsets is None:
        raise web.HTTPNotFound()
    print(offsets)
    print(topic_partition)

    if offsets[topic_partition] is None:
        raise web.HTTPNotFound()
    offset = offsets[topic_partition].offset
    print("offset: ", offset)
    consumer.seek(topic_partition, offset)
    print("right before encoding", topic)
    data = topic.encode('utf-8')  # Maybe just .encode()?
    print("after encoding", data)
    print("start reading messages")
    counter = 0
    try:
        async for msg in consumer:
            counter += 1
            #print(counter)
            if msg.timestamp > end:
                break
            #print("timestamp and value", msg.timestamp, msg.value)
            data += msg.value
    finally:
        print("done reading messages")
        await consumer.stop()
        # print("then we have", data)
    return web.Response(body=data)


