import os
import re
import email
import email.iterators
from sys import getsizeof
from glob import glob
from pprint import pprint
from itertools import chain
from functools import wraps
from glob import glob
import mmap


file_path = "Z:\\xxx\\xxx\\xxx"

if os.path.exists(file_path):
	print 'path exists'
	os.chdir(file_path)
else:
	print 'path not found'

print 'current path: {}'.format(os.getcwd())

# --- create a list of .eml files
eml_list = glob('*.eml')	# 29965 .eml files
current_email = None
subparts = []
#email_dict = dict()
email_counter = 0

def open_email():
	global current_email, email_counter, eml_list
	eml_list[email_counter]
	with open(eml_list[email_counter], 'rb') as f:
		mm_file =  mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
		current_email = email.message_from_file(mm_file)
	email_counter += 1

def extract_email_contents(write_path, num_files=10):
	global current_email, email_counter, subparts
	while email_counter < num_files:
		pattern = re.compile(r'\>(.+?)\<')
		
		o = open_email()
		contents = email \
				.iterators \
				.body_line_iterator(current_email, decode=True)
		temp = []
		for part in contents:
			temp.append(part)

		# --- clean the raw text 
		raw_text = ''.join(temp)
		begin = raw_text.find('<html>')
		end = raw_text.rfind('</html>')
		substring = raw_text[begin:end]
		#result = pattern.findall(substring)
		parsed_email = subparts.append(''.join(temp))
		currentemail = "{}".format(eml_list[email_counter])
		f_name = write_path + '\\' + currentemail +".txt"
		#os.chdir(write_path)
		f = open(f_name, 'w')
		f.write(substring)
		f.close()
		print("read {} files into storage".format(email_counter))
		print("wrote {} for path".format(f_name))

			



def main():
	global current_email
	#f = "Re_ Preferred Mutual Billing      Insured_ Daniel Vigeant         Gen Re No. 1000083601 BP6051 Act_242726 spec_ 25614.eml"
	wf = "N:\\xxx\\xxx\xxx\\xxx\\xxx\\exxx\\xxx"
	extract_email_contents(wf, 100)
	
	

if __name__ == '__main__':
	main()
