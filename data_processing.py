import pandas as pd

input_file = '20220709.SALDO_LIST_MIKRO_RKA'
data_awal = pd.read_csv(input_file+'.csv', delimiter="|", low_memory=False)
df_surabaya = data_awal[data_awal.KANWIL == 'KANWIL SURABAYA']
list_gadai = ['1601:ARRUM EMAS BARU', '1701:ARRUM HAJI', '2501:ARRUM HAJI TABUNGAN EMAS', '1901:ARRUM SAFAR', '1901:ARRUM SAFAR', '2901:EMASKU', '3801:EMASKU ULTIMATE KONVEN', '2801:EMASKU ULTIMATE SYARIAH', '3001:PINJAMAN MODAL KERJA', '3703:MULIA ULTIMATE KONVEN KOLEKTIF', '4301:GADAI TITIPAN EMAS', '0401:KRASIDA', '3501:KRASIDA TABUNGAN EMAS', '4001:KRESNA KHUSUS', '4401:MITRA GADAI', '3904:MULIA ARISAN', '3901:MULIA BARU', '0901:MULIA LAMA', '3701:MULIA ULTIMATE KONVEN', '3704:MULIA ULTIMATE KONVEN ARISAN', '3703:MULIA ULTIMATE KONVEN KOLEKTIF', '3702:MULIA ULTIMATE KONVEN PEGAWAI', '2701:MULIA ULTIMATE SYARIAH', '2704:MULIA ULTIMATE SYARIAH ARISAN', '2702:MULIA ULTIMATE SYARIAH PEGAWAI', '4801:PINJAMAN MODAL PRODUKTIF']
df_gadai = df_surabaya.loc[df_surabaya['SUB_PRODUK'].isin(list_gadai)]
df_non_gadai = df_surabaya.loc[~df_surabaya['SUB_PRODUK'].isin(list_gadai)]
df_surabaya2 = df_non_gadai[df_non_gadai.AREA == 'AREA SURABAYA 2']
# df_surabaya2.to_csv("surabaya2.csv")