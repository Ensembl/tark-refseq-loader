##### REQUIRED PACKAGES ############
### install sqlalchemy and pymysql  #
####################################

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import hashlib
import binascii
import traceback
import utils


def update_ref_seq_transcript_checksum(session):

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
        current_hex = binascii.hexlify(transcript_checksum).decode('utf-8').upper()
        print('curr hex ', current_hex)

        checksum = utils.generate_checksum('utf-8', loc_checksum, stable_id, stable_id_version, exon_set_checksum, seq_checksum, biotype)
        print('second calculation.....')
        new_transcript_checksum = utils.generate_checksum('utf-8', checksum)

        # convert to binary format
        # utils.update_transcript_checksum(session, transcript_id, binascii.unhexlify(new_transcript_checksum))
        print('new hex ', new_transcript_checksum)
        print("Same Transcript Checksum") if current_hex == new_transcript_checksum else print("New Transcript Checksum")
        print("\n")


def update_ensembl_transcript_checksum(session):

    # Execute the query and fetch all results
    query = text("""
                SELECT transcript_id, stable_id, stable_id_version, loc_checksum,
                 exon_set_checksum, seq_checksum, biotype, transcript_checksum
                FROM transcript
                INNER JOIN transcript_release_tag on transcript.transcript_id = transcript_release_tag.feature_id
                INNER JOIN release_set on transcript_release_tag.release_id = release_set.release_id
                INNER JOIN release_source on release_set.source_id = release_source.source_id
                WHERE release_source.shortname = 'Ensembl'
            """)

    results = session.execute(query)

    for row in results:
        (transcript_id, stable_id, stable_id_version, loc_checksum,
         exon_set_checksum, seq_checksum, biotype, transcript_checksum) = row

        print('inputs ', loc_checksum, stable_id, stable_id_version, exon_set_checksum, seq_checksum, biotype, '\n')

        print('curr transcript bin ', transcript_checksum)
        current_transcript_hex = binascii.hexlify(transcript_checksum).decode('utf-8').upper()
        print('curr transcript hex ', current_transcript_hex)

        #checksum_attributes = utils.remove_undefs(loc_checksum, stable_id, stable_id_version, exon_set_checksum, seq_checksum, biotype)
        # TODO: Use the above line
        checksum_attributes = utils.remove_undefs(loc_checksum, stable_id, stable_id_version, exon_set_checksum, seq_checksum)

        new_transcript_checksum = utils.generate_checksum('utf-8', *checksum_attributes)

        # convert to binary format
        # utils.update_transcript_checksum(session, transcript_id, binascii.unhexlify(new_transcript_checksum))
        print('new hex ', new_transcript_checksum)
        print("Same Transcript Checksum") if current_transcript_hex == new_transcript_checksum else print("New Transcript Checksum")
        print("\n")



if __name__ == '__main__':
    print('Updating transcript checksums')

    print('connecting to database...')
    host = 'mysql-ens-tark-rel'
    user = 'ensro'
    password = ''
    port = '4650'
    databases = 'ensembl_tark_e75_to_e104_biotype_fix'
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{databases}')  # Use your database connection string
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        session.begin()
        #update_ref_seq_transcript_checksum(session)
        update_ensembl_transcript_checksum(session)
        print('Checksums updated')
        session.commit()
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        session.rollback()
    finally:
        session.close()