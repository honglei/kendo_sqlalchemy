# -*- coding: utf-8 -*-
#

from sqlalchemy.sql.expression import asc,desc,or_,and_

from jquery_param import jquery_unparam

def get_filter_expression(one_filter,table_class ):
    field = one_filter.get('field')
    operator = one_filter.get('operator')
    value = one_filter.get('value')    
    express = None
    if(field and operator and value):
        db_field = getattr(table_class, field)
        if 'eq' == operator:
            express = (db_field ==  value)
        elif 'neq' == operator:
            express =  (db_field !=  value)
        elif 'lt' == operator:
            express = (db_field <  value)        
        elif 'lte' == operator:
            express = (db_field <= value)     
        elif 'gt' == operator:
            express = (db_field > value)                          
        elif 'gte' == operator:
            express = (db_field >=  value)  
        elif   operator in ( "startswith", "endswith", "contains" ):
            express = ( getattr( db_field , operator)(value) )  
    return express
def kendo_read(url, db_session, table_class ):
    '''
      db_session:  sqlalchemy.orm.scoped_session / essionmaker
      table_class: sqlalchemy.ext.declarative.declarative_base or 
      
      Support : Server-side  paging/sorting/filters
    '''
    q = db_session.query(table_class)
    args = jquery_unparam(url)
    if not args:
        return q, q.count()
    
    ### server-side sort
    sorts = args.get('sort')
    if(sorts): #[{u'field': u'age', u'dir': u'desc'}]
        for one_sort in sorts:
            field = one_sort.get('field')
            order  = one_sort.get('dir')
            if 'desc' ==order:
                q = q.order_by( desc(getattr(table_class, field)) )
            elif  'asc' == order:
                q = q.order_by( asc(getattr(table_class, field)) )
    ### server-side filter
    '''
     The supported operators are: 
     "eq" (equal to), "neq" (not equal to), 
     "lt" (less than), "lte" (less than or equal to), 
     "gt" (greater than), "gte" (greater than or equal to), 
     "startswith", "endswith", "contains". The last three are supported only for string fields.
    '''
    # args['filter']['filters']  [{u'operator': u'startswith', u'field': u'name', u'value': u'userB'}]
    filter_query =  args.get('filter')
    if(filter_query):
        logic = filter_query.get('logic')
        filters = filter_query.get('filters')
        expressions =[get_filter_expression(one_filter, table_class)
                      for one_filter in filters]
        if('or' == logic):
            q= q.filter(or_( *expressions))
        if('and' == logic):
            q= q.filter(and_( *expressions))        
    total = q.count()
    if('pageSize' in args):
        #pageSize = args.get('pageSize')
        skip = args.get('skip')
        #page = args.get('page')
        take = args.get('take')
        if(skip  and take):
            skip, take = int(skip),int(take)
            q = q[skip:skip+take]
    return q, total

def table_primay_keys(table_class):
    primary_keys =[]
    for column in table_class._sa_class_manager.attributes:
        #print column.class_.__name__, column.key
        if column.primary_key:
            primary_keys.append(column.key)
    return primary_keys

def item_has_primay_key(table_class, item ):
    bRet = True
    for column in table_class._sa_class_manager.attributes:
        if column.primary_key:
            if( column.key not in item):#item不为None，返回item中是否有主见
                bRet = False
    return bRet

def kendo_destroy(models,db_session,table_class):
    db_items=[]
    primay_keys = table_primay_keys(table_class)
    for item in models:
        q = find_matched_row(db_session, table_class, item, primay_keys=primay_keys)      
        user = q.first() 
        if user:
            q.delete()
            db_items.append(user)
    db_session.commit()  
    return db_items

def kendo_create(models,db_session,table_class):
    '''
        通过SQLAlchemy的DB定义来获得一个表的主键，并以此确定该操作
    '''
    db_items =[]
    primary_keys= table_primay_keys(table_class)
    for item in models:
        user = table_class()
        for key,value in item.iteritems():
            if hasattr(user, key) and key not in primary_keys :
                setattr(user,key,value)   
        db_session.add(user) 
        db_items.append(user)
    db_session.commit()  
    return db_items

import re
import datetime
from dateutil.parser import parse
from sqlalchemy import Integer,SmallInteger,Float,DateTime,DECIMAL,String
def json2dbtype(table_class, item_key, item_value):
    '''
    
    item_key,item_value： 从json中读取的属性名和属性值
    table_class ： 该属性对应的DB table类
    
    目的: 从Web前端获取的数据类型未必与DB table的数据类型匹配，在此进行格式转换
    1, 从Table中获取该item_key属性的类型
    2，将 item_value 转换为该类型
    '''
    db_attribute = getattr(table_class, item_key) 
    if isinstance( db_attribute.type , Integer) or isinstance(db_attribute.type,SmallInteger) :
        if re.match("\d+",item_value):
            return int(item_value)
    elif isinstance( db_attribute.type, DateTime):
        #dateutil.parser.parse('2011-05-25T20:34:05.787Z')
        return parse(item_value)
    elif isinstance( db_attribute.type, String):
        return str(item_value)
        
     


def find_matched_row(db_session, table_class, item,primay_keys=None):
    '''
      根据item的内容找到相应的数据库条目， 
      约定： item必须有
    '''
    if not primay_keys:
        primay_keys = table_primay_keys(table_class)
    # 通过主键找到对应的条目，然后进行更新
    if primay_keys:#table 有主键
        if item_has_primay_key(table_class, item): #item也有条目所需的内容
            q = db_session.query(table_class)
            expressions =[ getattr(table_class, primay_key) == 
                           json2dbtype(table_class,  primay_key , item.get(primay_key))
                          for primay_key in primay_keys]
            q= q.filter(and_( *expressions))    
    return q

def kendo_update(models,db_session,table_class):
    '''
    process models in url or request body
    TODO： 应该通过SQLAlchemy的DB定义来获得一个表的主键，并以此确定该操作
    '''
    db_items =[] 
    primay_keys = table_primay_keys(table_class)
    for item in models:
        q = find_matched_row(db_session, table_class, item,primay_keys=primay_keys)        
        user = q.first()
        if user:
            for key,value in item.iteritems():
                if hasattr(user, key) and key not in primay_keys:
                    setattr(user,key,value)   
            db_items.append(user)
    db_session.commit()  
    return db_items

if __name__ == "__main__":
    pass 
