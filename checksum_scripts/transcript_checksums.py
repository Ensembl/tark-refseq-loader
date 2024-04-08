##### REQUIRED PACKAGES ############
### install sqlalchemy and pymysql  #
####################################

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import hashlib
import binascii
import traceback


def update_checksum():
    print('connecting to database...')

    databases = ['ensembl_tark_e75_to_e104_biotype_fix']

    for db in databases:
        print('dbName ', db)
        engine = create_engine(f'mysql+pymysql://USER:PASSWORD@HOST:PORT/{db}')   # Use your database connection string
        Session = sessionmaker(bind=engine)
        session = Session()
        try:
            session.begin()

            # Execute the query and fetch all results
            query = text("""
                        SELECT loc_checksum, biotype, stable_id, stable_id_version, exon_set_checksum, seq_checksum, transcript_checksum, transcript_id
                        FROM transcript
                        WHERE transcript_id IN (
                            SELECT transcript_release_tag.feature_id
                            FROM transcript_release_tag
                            WHERE transcript_release_tag.release_id IN (
                                SELECT release_set.release_id
                                FROM release_set
                                WHERE source_id = 2
                            )
                        )
                    """)

            results = session.execute(query)

            for row in results:
                loc_checksum, biotype, stable_id, stable_id_version, exon_set_checksum, seq_checksum, transcript_checksum, transcript_id = row
                print('inputs ', loc_checksum, biotype, stable_id, stable_id_version, exon_set_checksum, seq_checksum, transcript_checksum, transcript_id, '\n')
                print('curr bin ', transcript_checksum)
                print('curr hex ', binascii.hexlify(transcript_checksum).decode('utf-8').upper())

                checksum = checksum_list('utf-8', loc_checksum, stable_id, stable_id_version, exon_set_checksum, seq_checksum, biotype)
                print('second calculation.....')
                new_transcript_checksum = checksum_list('utf-8', checksum)

                # convert to binary format
                update_transcript_checksum(session, transcript_id, binascii.unhexlify(new_transcript_checksum))
                print('new hex ', new_transcript_checksum)
                print("\n")

            session.commit()
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            session.rollback()
        finally:
            session.close()


def checksum_list(enc, *attr_list):
    cs = hashlib.sha1()   # update to other latest sha implementations

    main_list = []
    for item in attr_list:
        if isinstance(item, bytes):
            # Convert binary strings to hexadecimal strings
            main_list.append(binascii.hexlify(item).decode(enc).upper())
            # main_list.append(str(item))
        else:
            main_list.append(str(item))

    if len(main_list) > 1:
        main_list_joined = ':'.join(main_list)
    elif len(main_list) == 1:
        main_list_joined = main_list[0]
    else:
        return None

    if main_list_joined is not None:
        cs.update(main_list_joined.encode('utf-8').strip())
        hex_digest = binascii.hexlify(cs.digest()).decode('ascii').upper()

        return hex_digest
    else:
        return None

def update_transcript_checksum(session, transcript_id, new_checksum):
    # Create the SQL statement
    sql = text("""
                UPDATE transcript
                SET transcript_checksum = :checksum
                WHERE transcript_id = :transcript_id
            """)

    # Execute the SQL statement
    session.execute(sql, {"checksum": new_checksum, "transcript_id": transcript_id})


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('Updating checksums')
    update_checksum()
    print('Checksums updated')
