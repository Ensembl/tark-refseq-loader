### This is a general purpose script used to carryout various analysis on Tark DB ###
###
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

        checksum_attributes = utils.remove_undefs(loc_checksum, stable_id, stable_id_version, exon_set_checksum, seq_checksum, biotype)
        # TODO: Use the above line
        # checksum_attributes = utils.remove_undefs(loc_checksum, stable_id, stable_id_version, exon_set_checksum, seq_checksum)

        new_transcript_checksum = utils.generate_checksum('utf-8', *checksum_attributes)

        # convert to binary format
        utils.update_transcript_checksum(session, transcript_id, binascii.unhexlify(new_transcript_checksum))
        print('new hex ', new_transcript_checksum)
        print("Same Transcript Checksum") if current_transcript_hex == new_transcript_checksum else print("New Transcript Checksum")
        print("\n")

    
def remove_duplicates(session):
    # Execute the query and fetch all results
    query = text("""
                SELECT transcript.transcript_id, transcript.stable_id, transcript.stable_id_version, transcript.loc_checksum,
                transcript.exon_set_checksum, transcript.seq_checksum, transcript.biotype, transcript.transcript_checksum, 
                transcript.assembly_id, transcript.loc_start, transcript.loc_end, transcript.loc_strand, transcript.loc_region
                FROM transcript
                INNER JOIN transcript_release_tag ON transcript.transcript_id = transcript_release_tag.feature_id
                INNER JOIN release_set ON transcript_release_tag.release_id = release_set.release_id
                INNER JOIN release_source ON release_set.source_id = release_source.source_id
                WHERE release_source.shortname = 'Ensembl'                
            """)


    results = session.execute(query)

    for row in results:
        (transcript_id, stable_id, stable_id_version, loc_checksum,
        exon_set_checksum, seq_checksum, biotype, transcript_checksum,
            assembly_id, loc_start, loc_end, loc_strand, loc_region) = row

        find_all_transcript_query = text("""SELECT transcript.transcript_id, transcript.stable_id, transcript.stable_id_version, transcript.loc_checksum,
                                            transcript.exon_set_checksum, transcript.seq_checksum, transcript.biotype,
                                            transcript.assembly_id, transcript.loc_start, transcript.loc_end, transcript.loc_strand, transcript.loc_region
                                            FROM transcript
                                            WHERE stable_id = :stable_id
                """)

        # Execute the SQL statement
        transcripts = session.execute(find_all_transcript_query, {"stable_id": stable_id})

        current_transcript = utils.convert_vars('utf-8', stable_id, stable_id_version, loc_checksum,
                                    exon_set_checksum, seq_checksum, biotype,
                                    assembly_id, loc_start, loc_end, loc_strand, loc_region)

        # print(stable_id)
        count = 0
        for trs in transcripts:
            # print("in loop ", trs.stable_id)
            tested_transcript = utils.convert_vars('utf-8', trs.stable_id, trs.stable_id_version, trs.loc_checksum,
                    trs.exon_set_checksum, trs.seq_checksum, trs.biotype, trs.assembly_id, trs.loc_start, trs.loc_end, trs.loc_strand, trs.loc_region)

            # print('current ', current_transcript)
            # print('tested_ ', tested_transcript)
            if current_transcript.lower() == tested_transcript.lower():
                # print('YES')
                count =+ 1
                # print('count ', count)
                if count >= 2:
                    print('current ', current_transcript)
                    print('tested_ ', tested_transcript)
                    print('YES')
            # else:
            #     print('NO')
        # print('\n')
        # print("after loop ", stable_id)
    print('Done')





if __name__ == '__main__':
    print('Updating transcript checksums')

    print('connecting to database...')
    host = 'mysql-ens-tark-rel'
    user = 'ensro'
    password=""
    port = '4650'
    databases = 'ensembl_tark_e75_to_e105_biotype_fix'
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{databases}')  # Use your database connection string
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        session.begin()
        #update_ref_seq_transcript_checksum(session)
        remove_duplicates(session)
        # print('Checksums updated')
        session.commit()
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        session.rollback()
    finally:
        session.close()