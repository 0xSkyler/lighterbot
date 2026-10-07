"""Local web control panel.

A separate process from the trading service. It reads the status snapshot,
journal and log that the bot already writes, and controls the bot only through
the service manager (start/stop/restart) and the operator command channel in
``scalper.control`` (pause/resume/flatten). It never signs or submits orders
itself and it shares no event loop with the trading path.
"""
