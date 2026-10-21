# gow3-coop-relay

Tiny room relay for the God of War III Remastered (PC port) online co-op mod.
Players connect over WebSocket; one hosts and gets a 6-character join code, up to three others
join with it. Game packets are forwarded to the rest of the room. Python 3 standard library only.

Run locally: `python relay.py` (port from `$PORT`, default 10000).
