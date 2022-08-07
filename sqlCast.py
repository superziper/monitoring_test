from operator import index
import pandas as pd 
import mysql.connector

df_all = pd.read_csv('ALL.csv').fillna(0)

mydb = mysql.connector.connect(
    host ="localhost",
    user = "pegadaian",
    password = "pegadaian123",
    database = "pegadaian_test"
)

def createTable(table):
    mycursor = mydb.cursor()
    sql = "CREATE TABLE '%s' (TGL_DATA DATE, CIF VARCHAR(255), NO_KONTRAK VARCHAR(255), CUSTOMER_NM VARCHAR(255), ALAMAT VARCHAR(255), SUB_PRODUK VARCHAR(255), KECAMATAN VARCHAR(255), KABUPATEN VARCHAR(255), PROPINSI VARCHAR(255), TELP VARCHAR(255), HP VARCHAR(255), TGL_KREDIT DATE, TENOR INT(255), TGL_JATUH_TEMPO DATE, KOLEKTIBILITAS SMALLINT(255), HARI_TUNGGAKAN INT(255), UP INT(255), ANGSURAN INT(255), OSL INT(255), SALDO_REK_TITIPAN INT(255), SISA_POKOK INT(255), TUNGGAKAN_POKOK INT(255), TUNGGAKAN_SM INT(255), TUNGGAKAN_DENDA INT(255), STATUS_REK VARCHAR(255), FLAG_RESTRUK VARCHAR(255), FLAG_LEPAS_RESTRUK VARCHAR(255), KETERANGAN VARCHAR(255), STATUS_KLAIM_ASURANSI VARCHAR(255), OUTLET_y VARCHAR(255), CABANG_y VARCHAR(255), UBM_y VARCHAR(255), CBM_y VARCHAR(255))" %(table)
    mycursor.execute(sql)
    mydb.commit()

# createTable()

def insertValue(TGL_DATA, CIF, NO_KONTRAK, CUSTOMER_NM, ALAMAT, SUB_PRODUK, KECAMATAN, KABUPATEN, PROPINSI, TELP, HP, TGL_KREDIT, TENOR, TGL_JATUH_TEMPO, KOLEKTIBILITAS, HARI_TUNGGAKAN, UP, ANGSURAN, OSL, SALDO_REK_TITIPAN, SISA_POKOK, TUNGGAKAN_POKOK, TUNGGAKAN_SM, TUNGGAKAN_DENDA, STATUS_REK, FLAG_RESTRUK, FLAG_LEPAS_RESTRUK, KETERANGAN, STATUS_KLAIM_ASURANSI, OUTLET_y, CABANG_y, UBM_y, CBM_y):
    mycursor = mydb.cursor()
    sql = "INSERT INTO SURABAYA_ALL (TGL_DATA, CIF, NO_KONTRAK, CUSTOMER_NM, ALAMAT, SUB_PRODUK, KECAMATAN, KABUPATEN, PROPINSI, TELP, HP, TGL_KREDIT, TENOR, TGL_JATUH_TEMPO, KOLEKTIBILITAS, HARI_TUNGGAKAN, UP, ANGSURAN, OSL, SALDO_REK_TITIPAN, SISA_POKOK, TUNGGAKAN_POKOK, TUNGGAKAN_SM, TUNGGAKAN_DENDA, STATUS_REK, FLAG_RESTRUK, FLAG_LEPAS_RESTRUK, KETERANGAN, STATUS_KLAIM_ASURANSI, OUTLET_y, CABANG_y, UBM_y, CBM_y) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" %(TGL_DATA, CIF, NO_KONTRAK, CUSTOMER_NM, ALAMAT, SUB_PRODUK, KECAMATAN, KABUPATEN, PROPINSI, TELP, HP, TGL_KREDIT, TENOR, TGL_JATUH_TEMPO, KOLEKTIBILITAS, HARI_TUNGGAKAN, UP, ANGSURAN, OSL, SALDO_REK_TITIPAN, SISA_POKOK, TUNGGAKAN_POKOK, TUNGGAKAN_SM, TUNGGAKAN_DENDA, STATUS_REK, FLAG_RESTRUK, FLAG_LEPAS_RESTRUK, KETERANGAN, STATUS_KLAIM_ASURANSI, OUTLET_y, CABANG_y, UBM_y, CBM_y)
    mycursor.execute(sql)
    mydb.commit()

# for index, row in df_all.reset_index().iterrows():
#     TGL_DATA = row['TGL_DATA']
#     CIF = row['CIF']
#     NO_KONTRAK = row['NO_KONTRAK']
#     CUSTOMER_NM = row['CUSTOMER_NM']
#     NO_KONTRAK = row['NO_KONTRAK']
#     ALAMAT = row['ALAMAT']
#     SUB_PRODUK = row['SUB_PRODUK']
#     KECAMATAN = row['KECAMATAN']
#     KABUPATEN = row['KABUPATEN']
#     PROPINSI = row['PROPINSI']
#     TELP = row['TELP']
#     HP = row['HP']
#     TGL_KREDIT = row['TGL_KREDIT']
#     TENOR = row['TENOR']
#     TGL_JATUH_TEMPO = row['TGL_JATUH_TEMPO']
#     KOLEKTIBILITAS = row['KOLEKTIBILITAS']
#     HARI_TUNGGAKAN = row['HARI_TUNGGAKAN']
#     UP = row['UP']
#     ANGSURAN = row['ANGSURAN']
#     OSL = row['OSL']
#     SALDO_REK_TITIPAN = row['SALDO_REK_TITIPAN']
#     SISA_POKOK = row['SISA_POKOK']
#     TUNGGAKAN_POKOK = row['TUNGGAKAN_POKOK']
#     TUNGGAKAN_SM = row['TUNGGAKAN_SM']
#     TUNGGAKAN_DENDA = row['TUNGGAKAN_DENDA']
#     STATUS_REK = row['STATUS_REK']
#     FLAG_RESTRUK = row['FLAG_RESTRUK']
#     FLAG_LEPAS_RESTRUK = row['FLAG_LEPAS_RESTRUK']
#     KETERANGAN = row['KETERANGAN']
#     STATUS_KLAIM_ASURANSI = row['STATUS_KLAIM_ASURANSI']
#     OUTLET_y = row['OUTLET_y']
#     CABANG_y = row['CABANG_y']
#     UBM_y = row['UBM_y']
#     CBM_y = row['CBM_y']
#     insertValue(TGL_DATA, CIF, NO_KONTRAK, CUSTOMER_NM, ALAMAT, SUB_PRODUK, KECAMATAN, KABUPATEN, PROPINSI, TELP, HP, TGL_KREDIT, TENOR, TGL_JATUH_TEMPO, KOLEKTIBILITAS, HARI_TUNGGAKAN, UP, ANGSURAN, OSL, SALDO_REK_TITIPAN, SISA_POKOK, TUNGGAKAN_POKOK, TUNGGAKAN_SM, TUNGGAKAN_DENDA, STATUS_REK, FLAG_RESTRUK, FLAG_LEPAS_RESTRUK, KETERANGAN, STATUS_KLAIM_ASURANSI, OUTLET_y, CABANG_y, UBM_y, CBM_y)
#     print(index)

def sum(): 
    list_monitoring = []
    mycursor = mydb.cursor(buffered=True)
    sql = "SELECT UBM_y, CBM_y, SUM(OSL) AS OSL_TOTAL, COUNT(UBM_y) FROM SURABAYA_ALL GROUP BY UBM_y"
    mycursor.execute(sql)
    mydb.commit()
    rows = mycursor.fetchall()
    for row in rows:
        list_monitoring.append([row[0], row[1], row[2], row[3]])
    return list_monitoring

list_monitoring = sum()
df_final = pd.DataFrame(list_monitoring, columns=['UBM', 'CBM', 'OSL_TOTAL', 'COUNT'])