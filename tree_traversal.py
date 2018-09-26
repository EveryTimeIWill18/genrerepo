#!/usr/bin/env python2.7
import os
import io
import sys
import re
import email
import glob
from pprint import pprint
from itertools import chain

# --- python tree search(code snippit)

root = e.get_payload()
current = root
current_next = None
decoded_parts = []

def recurive_search(n):
	"""get the decoded parts"""
	global decoded_parts
	current = n
	for node in current:
		if node.is_multipart():
			current = node.get_payload()
			return recurive_search(n=current)
		else:
			decoded_parts.append(node.get_payload(decode=True))
			return node.get_payload(decode=True)
