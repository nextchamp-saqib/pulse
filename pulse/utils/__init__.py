def decode(data):
	if isinstance(data, bytes):
		return data.decode("utf-8")
	elif isinstance(data, dict):
		return {decode(k): decode(v) for k, v in data.items()}
	elif isinstance(data, list | tuple | set):
		return [decode(item) for item in data]
	else:
		return data


def pretty_bytes(size):
	if size is None:
		return "N/A"
	if size < 1024:
		return f"{size} B"
	elif size < 1024**2:
		return f"{size / 1024:.2f} KB"
	elif size < 1024**3:
		return f"{size / (1024**2):.2f} MB"
	else:
		return f"{size / (1024**3):.2f} GB"
