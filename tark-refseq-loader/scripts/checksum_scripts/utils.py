from sqlalchemy import text
import hashlib
import binascii

def update_gene_checksum(session, gene_id, new_checksum):
    # Create the SQL statement
    sql = text("""
                UPDATE gene
                SET gene_checksum = :checksum
                WHERE gene_id = :gene_id
            """)

    # Execute the SQL statement
    session.execute(sql, {"checksum": new_checksum, "gene_id": gene_id})


def generate_checksum(enc, *attr_list):
    cs = hashlib.sha1()  # update to other latest sha implementations

    main_list = []
    for item in attr_list:
        if isinstance(item, bytes):
            # Convert binary strings to hexadecimal strings
            main_list.append(binascii.hexlify(item).decode(enc).upper())
            # main_list.append(str(item))
        elif item is None:
            continue
        else:
            main_list.append(str(item))

    if len(main_list) > 1:
        main_list_joined = ':'.join(main_list)
    elif len(main_list) == 1:
        main_list_joined = main_list[0]
    else:
        return None

    if main_list_joined is not None:
        print(main_list_joined)
        cs.update(main_list_joined.encode('utf-8').strip())
        hex_digest = binascii.hexlify(cs.digest()).decode('ascii').upper()

        return hex_digest
    else:
        return None
