import os
import pandas as pd
import urllib
from sqlalchemy import (create_engine, MetaData,
                        Table, Column, select, or_, and_, func, INT)
from pprint import pprint
import pickle
import paramiko
import socket
import socks
import glob

# --- sql connection setup
DRIVER = '{SQL Server}'
base_connection = "driver={};Server=USTRNTPD334;Database=PCNA_Claims;Trusted_Connection=yes".format(DRIVER)
connection_string = "mssql+pyodbc:///?odbc_connect=" + urllib.quote_plus(base_connection)
engine = create_engine(connection_string)
connection = engine.connect()

#pprint(engine.table_names()) # get the names of all tables in database

metadata = MetaData()
sa_claims = Table('SA_DMS_Mapping_All_1', metadata, autoload=True, autoload_with=engine)

# build eml query
stmt = select([sa_claims.c.claim_id, sa_claims.c.Files]) \
            .where(sa_claims.c.Formats == 'eml')

#df1 = pd.DataFrame([i for i in connection.execute(stmt)], columns=['Claim Id', 'Eml Files'])

#dfs = map(lambda x: pd.DataFrame([i for i in connection.execute(x)]), selector)
#set_df_cols = map(lambda x, y: (x, y), ['Claim Id', 'Claim Id'], ['Eml Files', 'Rtf Files'])



# --- get sql data and  build data frames
selector = map(lambda x: select([sa_claims.c.claim_id,
                                 sa_claims.c.Files]) \
                                .where(sa_claims.c.Formats == '{}'.format(x)),
               [u'eml', u'rtf', u'msw8', u'pdf', u'html', u'docx'])

create_dfs = map(lambda x, y: pd.DataFrame([i for i in connection.execute(x)],
                                           columns=['Claim Id', y]),
                selector, [u'Eml Files', u'Rtf Files', u'Msw8 Files', u'Pdf Files' ,u'Html Files', u'Docx Files'])

pkl_dir = u'N:\\USD\\Business Data and Analytics\\Will dev folder\\pickle_path\\SA_Claims_pkl'




# --- create a pickle file to send over server
o = open("data_frame_all_file_types.pickle", "wb")
pickle.dump(create_dfs, o)
o.close()



def make_sftp_conn(local_path, remote_path, chunksize, extension_type):
    """create an sftp connection to send files over a server"""

    sent_count = 0
    os.chdir(local_path)
    files_to_send = glob.glob("*.{}".format(extension_type))
    while sent_count < len(files_to_send):
        current_chunk = 0
        while current_chunk < chunksize:
            def sftp_conn(host, port, un, pw, local_path, file_to_send, remote_path):
                sock = socks.socksocket()
                sock.set_proxy(
                    proxy_type=None,
                    addr=host,
                    port=port,
                    username=str(un),
                    password=str(pw)
                )
                sock.connect((host, port))
                if socket.gethostname() is not None:
                    print("Host: {}".format(socket.gethostname()))
                    print(socket.getfqdn())
                    print("-------------")
                    # create transport
                    sftp = paramiko.Transport(sock)
                    sftp.connect(
                        username=str(un),
                        password=str(pw)
                    )
                    if sftp.is_alive():
                        print(sftp.is_alive())
                        print("sftp connection is alive")
                        # create client
                        client = paramiko.SFTPClient.from_transport(sftp)
                        client.put(localpath=file_full_path,
                                   remotepath=str(remote_path) + "/" + str(file_to_send))
            try:
                file_full_path = local_path + '\\' + files_to_send[sent_count]
                # --- call sftp_conn
                sftp_conn('10.1.48.58', 22, 'WMurphy', 'Welcome@2018', os.getcwd(), files_to_send[sent_count], remote_path)
                sent_count += 1
                current_chunk += 1
            except:
                print("ERROR: An aerror occured, continuing to write to remote desktop")
    print("read in a total of {} files".format(sent_count))




def main():
    lp = "N:\\USD\\Business Data and Analytics\\Will dev folder\\doc_pdf_files"
    rp = "/home/wmurphy/will_env/raw_doc_to_pdf_files"
    rem_zip_path = "/home/wmurphy/will_env/pickle_files"
    host = ''
    port = 22
    un = ''
    pw = ''
    chunk_size = 10
    ext = "pdf"
    zip_ext = 'zip'
    make_sftp_conn(local_path=lp, remote_path=rem_zip_path, chunksize=chunk_size, extension_type=zip_ext)

if __name__ == '__main__':
    main()
