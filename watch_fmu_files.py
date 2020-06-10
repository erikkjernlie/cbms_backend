
import os, sys

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))

import asyncio
from hachiko.hachiko import AIOWatchdog, AIOEventHandler
WATCH_DIRECTORY = '.\\files\\projects'


async def processFMUfile(fmuPath):
	# Process FMU file
	pass



class MyEventHandler(AIOEventHandler):
	"""Subclass of asyncio-compatible event handler."""
	async def on_created(self, event):
		# if FMU file
		if event.src_path.endswith(".fmu"):
			print("fmu file")
			await processFMUfile(event.src_path)



async def watch_fs(watch_dir):
	patterns = "*"
	ignore_patterns = ""
	ignore_directories = False
	case_sensitive = True
	evh = MyEventHandler()
	watch = AIOWatchdog(watch_dir, event_handler=evh, recursive=True)
	watch.start()
	while True:
		await asyncio.sleep(1)
	watch.stop()


asyncio.get_event_loop().run_until_complete(watch_fs(WATCH_DIRECTORY))

