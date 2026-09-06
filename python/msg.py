# msg.py
# Artem Suprun
# 09/04/2026

"""
Message protocol for the IRC application.

Packet format:

    4 bytes: command
    4 bytes: payload length
    N bytes: UTF-8 encoded payload (N cannot exceed MAX_DATA)

All integers use network byte order (big endian).
"""
