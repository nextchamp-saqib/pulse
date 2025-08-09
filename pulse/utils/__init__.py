def decode(data):
	if isinstance(data, bytes):
		return data.decode("utf-8")
	elif isinstance(data, dict):
		return {
			k.decode("utf-8") if isinstance(k, bytes) else k: v.decode("utf-8") if isinstance(v, bytes) else v
			for k, v in data.items()
		}
	elif isinstance(data, list):
		return [decode(item) for item in data]
	else:
		return data
