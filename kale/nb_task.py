import time

def success_file_written(randhash, success_file):
	try:
		with open(success_file) as fh:
			if fh.read().strip() == randhash:
				print("NB Done!")
				return True
			else:
				print("NB Still running...")
				return False
	except FileNotFoundError:
		raise ValueError("Success file not found!")

def nb_poll_success_file(randhash, success_file, poll_interval=60):
	while not success_file_written(randhash, success_file):
		time.sleep(poll_interval)

