import os
import socket
import socks
import paramiko


def load_data(root_path, file_name):
	"""
	Data to be transferred via sftp.
	"""
	if os.path.isfile(os.path.join(root_path, file_name)):
		return os.path.join(root_path, file_name)
	else:
		return None




def transfer_data(host, port, username, password, root, fn, remote_path):
	"""
	Transer data via sftp connvection.
	"""

	# - socket setup
	sock = socks.socksocket()
	sock.set_proxy(
		proxy_type=None,
		addr=host,
		port=port,
		username=username,
		password=password
	)

	# - connect the socket
	sock.connect((host, port))
	print("Host Name\n-------------")
	print(socket.gethostname())
	print('\n')
	print("Full Host Name\n--------------")
	print(socket.getfqdn())
	print("Host address\n------------")
	print(socket.gethostbyaddr(host))

	# - create sftp transport
	sftp = paramiko.Transport(sock)
	# - connect
	sftp.connect(
		username=username,
		password=password
	)
	if sftp.is_alive():
		print("connection is live")
		# - create client
		client = paramiko.SFTPClient.from_transport(sftp)

		# - load the data to be transferred
		data = load_data(root, fn)
		if data is not None:
			client.put(localpath=str(data), 
				remotepath=os.path.join(remote_path, fn))
		print("Finished transferring payload")

	# - close the connection
	client.close()
	sftp.close()
