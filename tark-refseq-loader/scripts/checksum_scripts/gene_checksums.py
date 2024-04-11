##### REQUIRED PACKAGES ############
### install sqlalchemy and pymysql  #
####################################

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import binascii
import traceback
import utils


def update_ref_seq_gene_checksum(session):

    # Execute the query and fetch all results
    query = text("""
                SELECT loc_checksum, biotype, stable_id, stable_id_version, gene_checksum, gene_id
                FROM gene
                WHERE gene_id IN (
                    SELECT gene_release_tag.feature_id
                    FROM gene_release_tag
                    WHERE gene_release_tag.release_id IN (
                        SELECT release_set.release_id
                        FROM release_set
                        WHERE source_id = 2
                    )
                )
            """)

    results = session.execute(query)

    for row in results:
        loc_checksum, biotype, stable_id, stable_id_version, gene_checksum, gene_id = row
        print('gene_id - ', gene_id, '---->', 'inputs - ', loc_checksum, ' - ', biotype, ' - ', stable_id, ' - ', stable_id_version)
        print('curr bin ', gene_checksum)
        current_hex = binascii.hexlify(gene_checksum).decode('utf-8').upper()
        print('curr hex ', current_hex)

        checksum = utils.generate_checksum('utf-8', loc_checksum, biotype, stable_id, stable_id_version)
        # print('second calculation.....')
        # Double hashing
        new_gene_checksum = utils.generate_checksum('utf-8', checksum)

        # convert to binary format
        # utils.update_gene_checksum(session, gene_id, binascii.unhexlify(new_gene_checksum))
        print('new hex ', new_gene_checksum)
        print("Same Gene Checksum") if current_hex == new_gene_checksum else print("New Gene Checksum")
        print("\n")


def update_ensembl_gene_checksum(session):

    # Execute the query and fetch all results
    query = text("""
                SELECT gene_id, stable_id, stable_id_version, loc_region, loc_start,
                 loc_end, loc_strand, loc_checksum, name_id, gene_checksum, biotype, 
                 gene.assembly_id, assembly.assembly_name
                FROM gene
                INNER JOIN assembly on gene.assembly_id = assembly.assembly_id
                INNER JOIN gene_release_tag on gene.gene_id = gene_release_tag.feature_id
                INNER JOIN release_set on gene_release_tag.release_id = release_set.release_id
                INNER JOIN release_source on release_set.source_id = release_source.source_id
                WHERE release_source.shortname = 'Ensembl'
            """)

    results = session.execute(query)

    for row in results:
        (gene_id, stable_id, stable_id_version, loc_region, loc_start,
         loc_end, loc_strand, loc_checksum, name_id, gene_checksum,
         biotype, assembly_id, assembly_name) = row


        print('gene_id - ', gene_id, '---->',
              'inputs - ', assembly_id, ' - ', loc_region, ' - ', loc_start, ' - ', loc_end, ' - ',
              loc_strand, ' - ', name_id, ' - ', stable_id, ' - ', stable_id_version, ' - ', biotype)


        print('curr bin ', gene_checksum)
        current_hex = binascii.hexlify(gene_checksum).decode('utf-8').upper()
        print('curr hex ', current_hex)

        # Remove None values
        checksum_attributes = utils.remove_undefs(assembly_id, loc_region, loc_start, loc_end,
              loc_strand, name_id, stable_id, stable_id_version, biotype)


        # Generate checksum
        new_gene_checksum = utils.generate_checksum(
            'utf-8', *checksum_attributes)

        # convert to binary format and update database
        #utils.update_gene_checksum(session, gene_id, binascii.unhexlify(checksum))
        print('new hex ', new_gene_checksum)
        print("Same Gene Checksum") if current_hex == new_gene_checksum else print("New Gene Checksum")
        print("\n")




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('Updating checksums')

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
        update_ref_seq_gene_checksum(session)
        update_ensembl_gene_checksum(session)
        print('Checksums updated')
        session.commit()
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        session.rollback()
    finally:
        session.close()
