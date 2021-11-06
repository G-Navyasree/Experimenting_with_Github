import json

def to_str(v):
    return '' if v==None else str(v)

class Sequence:

    def __init__(self,conn,seq_owner,seq_name):
        cur = conn.cursor()
        sql="select INCREMENT_BY from {}.{}".format(seq_owner,seq_name)
        cur.execute(sql)
        ar=cur.fetchone()
        cur.close()
        self.increment=ar[0]
        self.next_seq_no=-1
        self.last_available_seq_no=-1
        self.cur=conn.cursor()
        self.sql="select nextval('{}.{}')".format(seq_owner,seq_name)
        
        
    def next_value(self):
        next_seq_no=self.next_seq_no;
        last_available_seq_no=self.last_available_seq_no;
        if(next_seq_no>0 and next_seq_no<=last_available_seq_no):
            self.next_seq_no=next_seq_no+1
            return next_seq_no
        cur=self.cur
        cur.execute(self.sql)
        ar=cur.fetchone()
        next_seq_no=ar[0]
        self.next_seq_no=next_seq_no+1
        self.last_available_seq_no=next_seq_no+self.increment-1
        return next_seq_no
        
class DMLStatement:
    def __init__(self,conn,sql,param_names):
        self.cur=conn.cursor()
        self.sql=sql
        self.param_names=param_names
        self.batch=[]
        self.batch_mode=False
        
    def set_batch_mode(self):
        self.batch_mode=True    
        
    def persist(self,o):
        if self.batch_mode:
            self.add_batch(o)
        else:
            self.execute(o)
    
    def execute(self,o):
        cur=self.cur
        param_names=self.param_names
        ar=[ o.get(str(name).upper(),'') for name in param_names ]
        str_query = self.sql % tuple(ar)
        result=cur.execute(str_query)

    def add_batch(self,o):
        cur=self.cur
        param_names=self.param_names
        ar=[ o.get(name,None) for name in param_names ]
        batch=self.batch
        batch.append(ar)
        if(len(batch)>=1000):self.execute_batch()
        
    def execute_batch(self):
        cur=self.cur
        batch=self.batch
        result=cur.executemany(self.sql,batch)
        self.batch=[]
        print(result)

    def checkpoint(self):
        pass
        
    def finish(self):
        batch=self.batch
        if(batch):self.execute_batch()
        self.cur.close()

class ArrayReader():

    def __init__(self,ar):
        self.ar=ar
        self.data='\n'.join(ar)
        self.data_len=len(self.data)
        self.data_ptr=0
        
    def read(self,sz):
        len=self.data_len
        ptr=self.data_ptr
        if ptr+sz>len:sz=len-ptr
        self.data_ptr+=sz
        return self.data[ptr:ptr+sz]

    def readline(self,x):
        print('readline')
        assert(0)
        pass



class CopyFromInsertStatement():
    
    def __init__(self,conn,table_name,param_names):
        self.cur=conn.cursor()
        self.table_name=table_name
        self.param_names=param_names
        self.delimiter='\t'
        self.batch=[]

    def persist(self,o):
        param_names=self.param_names
        ar=[ to_str(o.get(name,None)) for name in param_names ]
        s=self.delimiter.join(ar)
        self.batch.append(s)
        
    def checkpoint(self,conn=None):
        if conn==None:
            cur=self.cur
        else:
            cur=conn.cursor()
        cur.copy_expert("COPY {} FROM STDIN WITH CSV NULL '' DELIMITER '{}' ENCODING 'UTF-8' QUOTE '\x1f'".format(self.table_name,self.delimiter),ArrayReader(self.batch))
        self.batch=[]
        if conn!=None:
            cur.close()
        
    def spec_conn_checkpoint(self,conn):  
        batch_cur=conn.cursor()  
        batch_cur.copy_from(ArrayReader(self.batch),self.table_name,sep='|',null='')
        self.batch=[]

    def finish(self):
        self.checkpoint()
        
        
    def spec_conn_finish(self,conn):
        self.spec_conn_checkpoint(conn)    

class Comparator:
    pass

class Table():

    def __init__(self,conn,table_owner,table_name):
        self.table_name=table_name
        self.table_owner=table_owner
        self.pk_fields=None
        cur=conn.cursor()
        sql='select ordinal_position,column_name, data_type from information_schema.columns where table_schema=%s and table_name=%s order by ordinal_position';
        cur.execute(sql,[table_owner,table_name])
        ar=cur.fetchall()
        names=[r[0] for r in cur.description]
        col_count=len(names)
        self.fields=[ dict(zip(names,r)) for r in ar] 
        sql="select constraint_name from information_schema.table_constraints where constraint_type='PRIMARY KEY' and table_schema=%s and table_name=%s"
        cur.execute(sql,[table_owner,table_name])
        r=cur.fetchone()
        if r:
            constraint_name=r[0]
            sql="select column_name from information_schema.key_column_usage where constraint_name=%s and table_schema=%s and table_name=%s order by ordinal_position"
            cur.execute(sql,[constraint_name,table_owner,table_name])
            self.pk_fields=frozenset([r[0] for r in cur.fetchall()])
            cur.close()
        


        
    def get_select_list(self,prefix='',exclude_cols=set()):
        select_expr=[]
        for f in self.fields:
            type=f['data_type']
            name=f['column_name']
            if name in exclude_cols:continue
            fq_name=name
            if prefix: fq_name=prefix+'.'+name
            if(type=='date'):
                select_expr.append("TO_CHAR({},'YYYYMMDD') {}".format(fq_name,name))
            elif type =='timestamp with time zone':
                select_expr.append('''TO_CHAR({},'YYYYMMDD"T"HH24MISSOF') {}'''.format(fq_name,name))
            else:
                select_expr.append(fq_name)
        return (','.join(select_expr))

    def get_select_column_names(self):
        col_names=[f['column_name'] for f in self.fields]
        return col_names
        
    def get_insert_statement(self,conn,stmt_type=0,skip_cols=[]):
        insert_expr=[]
        param_list=[]
        param_names=[]
        fi=0
        for f in self.fields:
            fi+=1
            type=f['data_type']
            name=f['column_name']
            if name in skip_cols:continue
            insert_expr.append(name)
            param_names.append(name)
            if(type=='date'):
                param_list.append("TO_DATE('%s','YYYY/MM/DD HH24:MI:SS')")
            elif type == 'character varying':
                param_list.append("\'%s\'")          
            else:
                param_list.append('%s')
        sql="INSERT INTO {}.{} ({}) VALUES({})".format(self.table_owner,self.table_name,",".join(insert_expr),",".join(param_list))
        
        if stmt_type==0:
            return DMLStatement(conn,sql,param_names)
        if stmt_type==1:
            return CopyFromInsertStatement(conn,'{}.{}'.format(self.table_owner,self.table_name),param_names)
        if stmt_type==3:
            return (param_list)


    def get_update_statement(self,conn):
        pk_fields=self.pk_fields
        update_expr=[]
        where_expr=[]
        param_list=[]
        param_names=[]
        where_params=[]
        fi=0
        for f in self.fields:
            fi+=1
            type=f['data_type']
            name=f['column_name']
                        
            if(type=='date'):
                expr="{}=TO_DATE(%s,'YYYYMMDD')".format(name)            
            else:
                expr="{}=%s".format(name)
            if name in pk_fields:
                where_expr.append(expr)
                where_params.append(name)
            else:
                update_expr.append(expr)
                param_names.append(name)
        param_names.extend(where_params)
        sql="UPDATE {}.{} SET {} WHERE {} ".format(self.table_owner,self.table_name,",".join(update_expr)," AND ".join(where_expr))
        return DMLStatement(conn,sql,param_names)
        
        
    def create_comparator(self,exclude_set=()):
        comparator=Comparator(self.fields,exclude_set)
        return comparator



class Comparator:
    def __init__(self,fields,exclude_set=()):
        field_names=[]
        for f in fields:
            name=f['column_name']
            if name in exclude_set: continue
            field_names.append(name)
        self.field_names=field_names
        
    def compare(self,o1,o2,diffs):
        diffs.clear()
        field_names=self.field_names
        for name in field_names:
            v1=o1.get(name,None)
            v2=o2.get(name,None)
            if v1==v2: continue 
            diffs.append({'name':name,'v1':v1,'v2':v2})
        if not diffs: return False
        return True




class DataExporter():
    
    default_separator='|'

    def __init__(self,conn,sql,separator=None):
        self.conn=conn
        self.sql=sql
        self.progress_callback=None
        self.progress_count=1000
        self.separator=separator if separator!=None else self.default_separator
        self.auto_close_fh=True
        self.record_serializer=None
        
    def set_options(self,progress_count=1000,progress_callback=None):    
        self.progress_callback=progress_callback
        self.progress_count=progress_count
        
    def create_json_serializer(self,prefix):

        separator=self.separator

        def json_serializer_0(r):
            s=json.dumps(r)
            return s
        
        ## If no prefix, return a shortcut function that returns the json encoded string
        
        if prefix==0:
            self.record_serializer=json_serializer_0
            return 

        ## If prefix is 1, return a shortcut function that returns the first column and json encoded string 

        def json_serializer_1(r):
            s=json.dumps(r)
            pr=r[0]
            return pr+separator+s
        
        if prefix==1:
            self.record_serializer=json_serializer_1
            return 
        
        ###For prefix length>1,need to build prefix array first, then join it with separator

        def json_serializer_n(r):
            s=json.dumps(r)
            a_prefix=r[0:prefix]
            return separator.join(a_prefix)+separator+s

        self.record_serializer=json_serializer_n

            


    def create_delimited_serializer(self):
        
        separator=self.separator
        
        def serializer(r):
            a=[to_str(f) for f in r]
            return separator.join(a)
        
        self.record_serializer=serializer
            
    

    def get_postgres_types(self):
        cur = self.conn.cursor()
        cur.execute('select oid,typname from pg_catalog.pg_type')
        postgres_types = {}
        for r in cur:
            postgres_types[r[0]] = r[1]
        cur.close()
        return postgres_types

    def create_meta(self,rs_desc=None,meta_hints={},meta_sql=None):
        
        
        if rs_desc==None:
        
            cur = self.conn.cursor()
            sql=meta_sql if meta_sql!=None else self.sql+' limit 0'
            cur.execute(sql)
            rs_desc=cur.description
            cur.close()

        postgres_types = self.get_postgres_types()
                
        meta=[]
        for r in rs_desc:
            name = r[0]
            field_meta={'name':name}
            if name in meta_hints:
                hint=meta_hints[name]
                field_meta.update(hint)
            else:
                type_code = r[1]
                pg_type_name = postgres_types[type_code]
                type_name = 'string'
                if pg_type_name == 'int4' or pg_type_name == 'int8':
                    type_name = 'integer'
                if pg_type_name == 'numeric':
                    type_name = 'decimal'
                field_meta['type']=type_name
            meta.append(field_meta)

        self.meta=meta

    def get_formatted_meta(self,meta_format='json'):
        if meta_format=='json':
            header=json.dumps(self.meta)
            return header
        return None
    
    
    def export_using_cursor(self,fh=None,cur_name='cur_data_export'):
        cur = self.conn.cursor(cur_name)
        cur.itersize = 1000
        cur.execute(self.sql)
        
        progress_callback=self.progress_callback
        progress_count=self.progress_count
        report_progress=True if progress_callback else False
        
        if self.record_serializer==None:
            self.create_delimited_serializer()

        record_serializer=self.record_serializer
        
        rc=0
        for r in cur:
            
            rc+=1
            if report_progress and rc%progress_count==0:
                progress_callback(rc)
            s=record_serializer(r)
            print(s,file=fh)
                        
        cur.close()
        if self.auto_close_fh:fh.close()

    def export_using_copy_to(self,fh=None,delimiter='\t',encoding='UTF-8',null_str='',quote='\x1f'):
        
        cur=self.conn.cursor()
        copy_cmd="COPY ({}) TO STDOUT WITH csv DELIMITER '{}' ENCODING '{}' NULL '{}'  QUOTE '{}'".format(self.sql,delimiter,encoding,null_str,quote) 
        cur.copy_expert(copy_cmd,fh)

        cur.close()
        if self.auto_close_fh:fh.close()




    
