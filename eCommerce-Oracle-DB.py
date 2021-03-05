#read from daily files and insert in landing zone table
import sys
import cx_Oracle
Cred = 'hr/hr@'
host = 'localhost'
sid ='orcl'
#connection to database
conn=cx_Oracle.connect(Cred + host + "/" + sid)
#create cursor
cur=conn.cursor()
try:
    trunc_lz_table="TRUNCATE TABLE LZ_Transaction"
    cur.execute(trunc_lz_table)
    i=int(input('Enter file day'))
    f = open('C:\$p@rK\Python_DB_POC\Day%d.txt'%i,"r")
    l1=[6,6,20,5,7,30,6,4,12,20,11,11,11]
    l2=[]
    for x in f:
        l3=[]
        current_position = 0
        for i in range(len(l1)):
            end_position = current_position + l1[i]
            l3.append(x[current_position:end_position].strip())
            current_position = end_position
        l2.append(l3)
    f.close()
    print(l2)
    cur.executemany("INSERT INTO LZ_Transaction (Transaction_Id, Customer_Id, Customer_Nm, Customer_Addr_Id, Product_Id, Product_Nm, Product_Price, Product_Quantity, Status, Transaction_TimeStamp, Ordered_Date, Shipped_Date, Delivered_Date) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)",l2)
    conn.commit()
except Exception as e:
    print("Error:",str(e))
    conn.rollback()
#truncate staging table to accept new data
try:
    trunc_st_table="truncate table st_transaction"
    cur.execute(trunc_st_table)
    conn.commit()

except Exception as e:
    print("Error:",str(e))
    conn.rollback()
#insert into stage table
try:
    insert_st_table="INSERT INTO ST_Transaction (Transaction_Key,Transaction_Id, Customer_Id, Customer_Name, Customer_Addr_Id, Product_Id, product_name, Product_Price, Product_Quantity, Status, Transaction_TimeStamp, Ordered_Date, Shipped_Date, Delivered_Date) SELECT BASE_TRANS_SEQ.NEXTVAL,Transaction_Id, Customer_Id, Customer_Nm, Customer_Addr_Id, Product_Id, Product_Nm, Product_Price, Product_Quantity, Status, TO_TIMESTAMP(Transaction_TimeStamp,'DD-MON-YYYY HH24:MI:SS'), TO_DATE(Ordered_Date,'DD-MON-YYYY'),TO_DATE(shipped_date,'DD-MON-YYYY'), TO_DATE(Delivered_Date,'DD-MON-YYYY') FROM lz_transaction"
    cur.execute(insert_st_table)
    cur.callproc('Action_indicator')
    update_action_ind="UPDATE ST_TRANSACTION SET action_ind = CASE WHEN TRANSACTION_ID IN (SELECT base_transaction.transaction_id FROM BASE_TRANSACTION) THEN 'U' ELSE 'I' END"
    cur.execute(update_action_ind)
    conn.commit()
    cur.close()
    conn.close()
except Exception as e:
    print("Error",str(e))
    conn.rollback()
