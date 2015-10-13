/* Copyright (c) 2004, 2013, Oracle and/or its affiliates.
   Copyright (c) 2010, 2014, SkySQL Ab.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file ha_spartan.cc

  @brief
  The ha_spartan engine is a stubbed storage engine for spartan purposes only;
  it does almost nothing at this point. Its purpose is to provide a source
  code illustration of how to begin writing new storage engines; see also
  storage/spartan/ha_spartan.h.

  Additionally, this file includes an spartan of a daemon plugin which does
  nothing at all - absolutely nothing, even less than spartan storage engine.
  But it shows that one dll/so can contain more than one plugin.

  @details
  ha_spartan will let you create/open/delete tables, but
  nothing further (for spartan, indexes are not supported nor can data
  be stored in the table). It also provides new status (spartan_func_spartan)
  and system (spartan_ulong_var and spartan_enum_var) variables.

  Use this spartan as a template for implementing the same functionality in
  your own storage engine. You can enable the spartan storage engine in your
  build by doing the following during your build process:<br> ./configure
  --with-spartan-storage-engine

  Once this is done, MySQL will let you create tables with:<br>
  CREATE TABLE <table name> (...) ENGINE=SPARTAN;

  The spartan storage engine is set up to use table locks. It
  implements an spartan "SHARE" that is inserted into a hash by table
  name. You can use this to store information of state that any
  spartan handler object will be able to see when it is using that
  table.

  Please read the object definition in ha_spartan.h before reading the rest
  of this file.

  @note
  When you create an SPARTAN table, the MySQL Server creates a table .frm
  (format) file in the database directory, using the table name as the file
  name as is customary with MySQL. No other files are created. To get an idea
  of what occurs, here is an spartan select that would do a scan of an entire
  table:

  @code
  ha_spartan::store_lock
  ha_spartan::external_lock
  ha_spartan::info
  ha_spartan::rnd_init
  ha_spartan::extra
  ENUM HA_EXTRA_CACHE        Cache record in HA_rrnd()
  ha_spartan::rnd_next
  ha_spartan::rnd_next
  ha_spartan::rnd_next
  ha_spartan::rnd_next
  ha_spartan::rnd_next
  ha_spartan::rnd_next
  ha_spartan::rnd_next
  ha_spartan::rnd_next
  ha_spartan::rnd_next
  ha_spartan::extra
  ENUM HA_EXTRA_NO_CACHE     End caching of records (def)
  ha_spartan::external_lock
  ha_spartan::extra
  ENUM HA_EXTRA_RESET        Reset database to after open
  @endcode

  Here you see that the spartan storage engine has 9 rows called before
  rnd_next signals that it has reached the end of its data. Also note that
  the table in question was already opened; had it not been open, a call to
  ha_spartan::open() would also have been necessary. Calls to
  ha_spartan::extra() are hints as to what will be occuring to the request.

  A Longer Spartan can be found called the "Skeleton Engine" which can be 
  found on TangentOrg. It has both an engine and a full build environment
  for building a pluggable storage engine.

  Happy coding!<br>
    -Brian
*/

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include <my_config.h>
#include <mysql/plugin.h>
#include "ha_spartan.h"
#include "sql_class.h"

static handler *spartan_create_handler(handlerton *hton,
                                       TABLE_SHARE *table, 
                                       MEM_ROOT *mem_root);

handlerton *spartan_hton;

static MYSQL_THDVAR_ULONG(varopt_default, PLUGIN_VAR_RQCMDARG,
  "default value of the VAROPT table option", NULL, NULL, 5, 0, 100, 0);

/**
  Structure for CREATE TABLE options (table options).
  It needs to be called ha_table_option_struct.

  The option values can be specified in the CREATE TABLE at the end:
  CREATE TABLE ( ... ) *here*
*/

struct ha_table_option_struct
{
  const char *strparam;
  ulonglong ullparam;
  uint enumparam;
  bool boolparam;
  ulonglong varparam;
};


/**
  Structure for CREATE TABLE options (field options).
  It needs to be called ha_field_option_struct.

  The option values can be specified in the CREATE TABLE per field:
  CREATE TABLE ( field ... *here*, ... )
*/

struct ha_field_option_struct
{
  const char *complex_param_to_parse_it_in_engine;
};

/*
  no spartan here, but index options can be declared similarly
  using the ha_index_option_struct structure.

  Their values can be specified in the CREATE TABLE per index:
  CREATE TABLE ( field ..., .., INDEX .... *here*, ... )
*/

ha_create_table_option spartan_table_option_list[]=
{
  /*
    one numeric option, with the default of UINT_MAX32, valid
    range of values 0..UINT_MAX32, and a "block size" of 10
    (any value must be divisible by 10).
  */
  HA_TOPTION_NUMBER("ULL", ullparam, UINT_MAX32, 0, UINT_MAX32, 10),
  /*
    one option that takes an arbitrary string
  */
  HA_TOPTION_STRING("STR", strparam),
  /*
    one enum option. a valid values are strings ONE and TWO.
    A default value is 0, that is "one".
  */
  HA_TOPTION_ENUM("one_or_two", enumparam, "one,two", 0),
  /*
    one boolean option, the valid values are YES/NO, ON/OFF, 1/0.
    The default is 1, that is true, yes, on.
  */
  HA_TOPTION_BOOL("YESNO", boolparam, 1),
  /*
    one option defined by the system variable. The type, the range, or
    a list of allowed values is the same as for the system variable.
  */
  HA_TOPTION_SYSVAR("VAROPT", varparam, varopt_default),

  HA_TOPTION_END
};

ha_create_table_option spartan_field_option_list[]=
{
  /*
    If the engine wants something more complex than a string, number, enum,
    or boolean - for spartan a list - it needs to specify the option
    as a string and parse it internally.
  */
  HA_FOPTION_STRING("COMPLEX", complex_param_to_parse_it_in_engine),
  HA_FOPTION_END
};


/**
  @brief
  Function we use in the creation of our hash to get key.
*/

#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key ex_key_mutex_Spartan_share_mutex;

static PSI_mutex_info all_spartan_mutexes[]=
{
  { &ex_key_mutex_Spartan_share_mutex, "Spartan_share::mutex", 0}
};

static void init_spartan_psi_keys()
{
  const char* category= "spartan";
  int count;

  count= array_elements(all_spartan_mutexes);
  mysql_mutex_register(category, all_spartan_mutexes, count);
}
#endif


/**
  @brief
  If frm_error() is called then we will use this to determine
  the file extensions that exist for the storage engine. This is also
  used by the default rename_table and delete_table method in
  handler.cc and by the default discover_many method.

  For engines that have two file name extentions (separate meta/index file
  and data file), the order of elements is relevant. First element of engine
  file name extentions array should be meta/index file extention. Second
  element - data file extention. This order is assumed by
  prepare_for_repair() when REPAIR TABLE ... USE_FRM is issued.

  @see
  rename_table method in handler.cc and
  delete_table method in handler.cc
*/

#define SDE_EXT	".sde"
#define SDI_EXT	".sdi"
static const char *ha_spartan_exts[] = {
  SDE_EXT,
  SDI_EXT,
  NullS
};

Spartan_share::Spartan_share()
{
  thr_lock_init(&lock);
  mysql_mutex_init(ex_key_mutex_Spartan_share_mutex,
                   &mutex, MY_MUTEX_INIT_FAST);
  data_class = new Spartan_data();
  index_class = new Spartan_index();
}


static int spartan_init_func(void *p)
{
  DBUG_ENTER("spartan_init_func");

#ifdef HAVE_PSI_INTERFACE
  init_spartan_psi_keys();
#endif

  spartan_hton= (handlerton *)p;
  spartan_hton->state=   SHOW_OPTION_YES;
  spartan_hton->create=  spartan_create_handler;
  spartan_hton->flags=   HTON_CAN_RECREATE;
  spartan_hton->table_options= spartan_table_option_list;
  spartan_hton->field_options= spartan_field_option_list;
  spartan_hton->tablefile_extensions= ha_spartan_exts;

  DBUG_RETURN(0);
}


/**
  @brief
  Spartan of simple lock controls. The "share" it creates is a
  structure we will pass to each spartan handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

Spartan_share *ha_spartan::get_share()
{
  Spartan_share *tmp_share;

  DBUG_ENTER("ha_spartan::get_share()");

  lock_shared_ha_data();
  if (!(tmp_share= static_cast<Spartan_share*>(get_ha_share_ptr())))
  {
    tmp_share= new Spartan_share;
    if (!tmp_share)
      goto err;

    set_ha_share_ptr(static_cast<Handler_share*>(tmp_share));
  }
err:
  unlock_shared_ha_data();
  DBUG_RETURN(tmp_share);
}

static handler* spartan_create_handler(handlerton *hton,
                                       TABLE_SHARE *table, 
                                       MEM_ROOT *mem_root)
{
  return new (mem_root) ha_spartan(hton, table);
}

ha_spartan::ha_spartan(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg)
{}


/**
  @brief
  Used for opening tables. The name will be the name of the file.

  @details
  A table is opened when it needs to be opened; e.g. when a request comes in
  for a SELECT on the table (tables are not open and closed for each request,
  they are cached).

  Called from handler.cc by handler::ha_open(). The server opens all tables by
  calling ha_open() which then calls the handler specific open().

  @see
  handler::ha_open() in handler.cc
*/

int ha_spartan::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER("ha_spartan::open");
  char name_buff[FN_REFLEN];
  
  if (!(share = get_share()))
    DBUG_RETURN(1);
  /*
	Call the data class open table method.
	Note: the fn_format() method correctly creates a file name from the
	name passed into the method.
  */
  share->data_class->open_table(fn_format(name_buff, name, "", SDE_EXT,
  								MY_REPLACE_EXT | MY_UNPACK_FILENAME));
  share->index_class->open_index(fn_format(name_buff, name, "", SDI_EXT,
  								MY_REPLACE_EXT | MY_UNPACK_FILENAME));
  share->index_class->load_index();
  thr_lock_data_init(&share->lock,&lock,NULL);

#ifndef DBUG_OFF
  ha_table_option_struct *options= table->s->option_struct;

  DBUG_ASSERT(options);
  DBUG_PRINT("info", ("strparam: '%-.64s'  ullparam: %llu  enumparam: %u  "\
                      "boolparam: %u",
                      (options->strparam ? options->strparam : "<NULL>"),
                      options->ullparam, options->enumparam, options->boolparam));
#endif

  DBUG_RETURN(0);
}


/**
  @brief
  Closes a table.

  @details
  Called from sql_base.cc, sql_select.cc, and table.cc. In sql_select.cc it is
  only used to close up temporary tables or during the process where a
  temporary table is converted over to being a myisam table.

  For sql_base.cc look at close_data_tables().

  @see
  sql_base.cc, sql_select.cc and table.cc
*/

int ha_spartan::close(void)
{
  DBUG_ENTER("ha_spartan::close");
  share->data_class->close_table();
  share->index_class->save_index();
  share->index_class->destroy_index();
  share->index_class->close_index();
  DBUG_RETURN(0);
}


/**
  @brief
  write_row() inserts a row. No extra() hint is given currently if a bulk load
  is happening. buf() is a byte array of data. You can use the field
  information to extract the data from the native byte array type.

  @details
  Spartan of this would be:
  @code
  for (Field **field=table->field ; *field ; field++)
  {
    ...
  }
  @endcode

  See ha_tina.cc for an spartan of extracting all of the data as strings.
  ha_berekly.cc has an spartan of how to store it intact by "packing" it
  for ha_berkeley's own native storage type.

  See the note for update_row() on auto_increments and timestamps. This
  case also applies to write_row().

  Called from item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc, and sql_update.cc.

  @see
  item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc and sql_update.cc
*/

int ha_spartan::write_row(uchar *buf)
{
  DBUG_ENTER("ha_spartan::write_row");
  /*
    Spartan of a successful write_row. We don't store the data
    anywhere; they are thrown away. A real implementation will
    probably need to do something with 'buf'. We report a success
    here, to pretend that the insert was successful.
  */
  long long pos;
  SDE_INDEX ndx;

  ha_statistic_increment(&SSV::ha_write_count);
  /*
	Begin critical section by locking the spartan mutex variable.
  */
  mysql_mutex_lock(&share->mutex);
  ndx.length = get_key_len();
  memcpy(ndx.key, get_key(), get_key_len());
  pos = share->data_class->write_row(buf, table->s->rec_buff_length);
  ndx.pos = pos;
  if((ndx.key != 0) && (ndx.length != 0))
  	share->index_class->insert_key(&ndx, false);
  /*
	End section by unlocking the spartan mutex variable.
  */
  mysql_mutex_unlock(&share->mutex);
  DBUG_RETURN(0);
}

int ha_spartan::rename_table(const char * from,const char * to)
{
  DBUG_ENTER("ha_spartan::rename_table");
  char data_from[FN_REFLEN];
  char data_to[FN_REFLEN];
  char index_from[FN_REFLEN];
  char index_to[FN_REFLEN];

  my_copy(fn_format(data_from, from, "", SDE_EXT,
  					MY_REPLACE_EXT | MY_UNPACK_FILENAME),
  		  fn_format(data_to, to, "", SDE_EXT,
  		  			MY_REPLACE_EXT | MY_UNPACK_FILENAME), MYF(0));
  my_copy(fn_format(index_from, from, "", SDI_EXT,
  					MY_REPLACE_EXT | MY_UNPACK_FILENAME),
  		  fn_format(index_to, to, "", SDI_EXT,
  		  			MY_REPLACE_EXT | MY_UNPACK_FILENAME), MYF(0));
  /*
	Delete the file using MySQL's delete file method.
  */
  my_delete(data_from, MYF(0));
  my_delete(index_from, MYF(0));
  DBUG_RETURN(0);
}

uchar *ha_spartan::get_key()
{
  uchar *key = 0;

  DBUG_ENTER("ha_spartan::get_key");
  /*
	For each field in the table, check to see if it is the key
	by checking the key_start variable. (1 = is a key).
  */
  for(Field **field = table->field; *field; field++) {
	if((*field)->key_start.to_ulonglong() == 1) {
	  /*
		Copy field value to key value (save key)
	  */
	  key = (uchar *)my_malloc((*field)->field_length,
	  							MYF(MY_ZEROFILL | MY_WME));
	  memcpy(key, (*field)->ptr, (*field)->key_length());
	}
  }
  DBUG_RETURN(key);
}

int ha_spartan::get_key_len()
{
  int length = 0;

  DBUG_ENTER("ha_spartan::get_key_len");
  /*
	For each field int the table, check to see if it is the key
	by checking the key_start variable. (1 = is a key).
  */
  for(Field **field = table->field; *field; field++) {
  	if((*field)->key_start.to_ulonglong() == 1)
	  /*
		Copy field length to key length
	  */
	  length = (*field)->key_length();
  }
  DBUG_RETURN(length);
}

/**
  @brief
  Yes, update_row() does what you expect, it updates a row. old_data will have
  the previous row record in it, while new_data will have the newest data in it.
  Keep in mind that the server can do updates based on ordering if an ORDER BY
  clause was used. Consecutive ordering is not guaranteed.

  @details
  Currently new_data will not have an updated auto_increament record, or
  and updated timestamp field. You can do these for spartan by doing:
  @code
  if (table->next_number_field && record == table->record[0])
    update_auto_increment();
  @endcode

  Called from sql_select.cc, sql_acl.cc, sql_update.cc, and sql_insert.cc.

  @see
  sql_select.cc, sql_acl.cc, sql_update.cc and sql_insert.cc
*/
int ha_spartan::update_row(const uchar *old_data, uchar *new_data)
{

  DBUG_ENTER("ha_spartan::update_row");
  /*
	Begin critical section by locking the spartan mutex variable.
  */
  mysql_mutex_lock(&share->mutex);
  share->data_class->update_row((uchar *)old_data, new_data,
  							table->s->rec_buff_length, current_position - 
  							share->data_class->row_size(table->s->rec_buff_length));
  if(get_key() != 0) {
	share->index_class->update_key(get_key(), current_position - share->data_class->row_size(table->s->rec_buff_length),
				        get_key_len());
	share->index_class->save_index();
	share->index_class->load_index();
  }
  /*
	End section by unlocking the spartan mutex variable.
  */
  mysql_mutex_unlock(&share->mutex);
  DBUG_RETURN(0);
  //DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  This will delete a row. buf will contain a copy of the row to be deleted.
  The server will call this right after the current row has been called (from
  either a previous rnd_nexT() or index call).

  @details
  If you keep a pointer to the last row or can access a primary key it will
  make doing the deletion quite a bit easier. Keep in mind that the server does
  not guarantee consecutive deletions. ORDER BY clauses can be used.

  Called in sql_acl.cc and sql_udf.cc to manage internal table
  information.  Called in sql_delete.cc, sql_insert.cc, and
  sql_select.cc. In sql_select it is used for removing duplicates
  while in insert it is used for REPLACE calls.

  @see
  sql_acl.cc, sql_udf.cc, sql_delete.cc, sql_insert.cc and sql_select.cc
*/

int ha_spartan::delete_row(const uchar *buf)
{
  long long pos;
  
  DBUG_ENTER("ha_spartan::delete_row");
  if(current_position > 0)
  	pos = current_position - 
  		share->data_class->row_size(table->s->rec_buff_length);
  else
  	pos = 0;
  mysql_mutex_lock(&share->mutex);
  share->data_class->delete_row((uchar *)buf, table->s->rec_buff_length, pos);
  if(get_key() != 0)
  	share->index_class->delete_key(get_key(), pos, get_key_len());
  mysql_mutex_unlock(&share->mutex);
  DBUG_RETURN(0);
  //DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Positions an index cursor to the index specified in the handle. Fetches the
  row if available. If the key value is null, begin at the first key of the
  index.
*/

int ha_spartan::index_read_map(uchar *buf, const uchar *key,
                               key_part_map keypart_map __attribute__((unused)),
                               enum ha_rkey_function find_flag
                               __attribute__((unused)))
{
  int rc;
  long long pos;
  DBUG_ENTER("ha_spartan::index_read");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  if(key == NULL)
  	pos = share->index_class->get_first_pos();
  else
  	pos = share->index_class->get_index_pos((uchar *)key, keypart_map);

  if(pos == -1)
  	DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
  current_position = pos + share->data_class->row_size(table->s->rec_buff_length);
  rc = share->data_class->read_row(buf, table->s->rec_buff_length, pos);
  share->index_class->get_next_key();
  MYSQL_INDEX_READ_ROW_DONE(rc);
  //rc= HA_ERR_WRONG_COMMAND;
  DBUG_RETURN(rc);
}


/**
  @brief
  Used to read forward through the index.
*/

int ha_spartan::index_next(uchar *buf)
{
  int rc;
  uchar *key = 0;
  long long pos;
  
  DBUG_ENTER("ha_spartan::index_next");
  MYSQL_INDEX_READ_ROW_START(table->share->db.str, table_share->table_name.str);
  key = share->index_class->get_next_key();
  if( key == 0)
  	DBUG_RETURN(HA_ERR_END_OF_FILE);
  pos = share->index_class->get_index_pos((uchar *)key, get_key_len());
  share->index_class->seek_index(key, get_key_len());
  share->index_class->get_next_key();
  if( pos == -1)
  	DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
  rc = share->data_class->read_row(buf, table->s->rec_buff_length, pos);
  MYSQL_INDEX_READ_ROW_DONE(rc);
  //rc= HA_ERR_WRONG_COMMAND;
  DBUG_RETURN(rc);
}


/**
  @brief
  Used to read backwards through the index.
*/

int ha_spartan::index_prev(uchar *buf)
{
  int rc;
  uchar *key=0;
  long long pos;
  
  DBUG_ENTER("ha_spartan::index_prev");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  key = share->index_class->get_prev_key();
  if(key == 0)
  	DBUG_RETURN(HA_ERR_END_OF_FILE);
  pos = share->index_class->get_index_pos((uchar *)key, get_key_len());
  share->index_class->seek_index(key, get_key_len());
  share->index_class->get_prev_key();
  if(pos == -1)
  	DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
  rc = share->data_class->read_row(buf, table->s->rec_buff_length, pos);
  MYSQL_INDEX_READ_ROW_DONE(rc);
  //rc= HA_ERR_WRONG_COMMAND;
  DBUG_RETURN(rc);
}


/**
  @brief
  index_first() asks for the first key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_spartan::index_first(uchar *buf)
{
  int rc;
  uchar *key=0;
  
  DBUG_ENTER("ha_spartan::index_first");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  key = share->index_class->get_first_key();
  if( key == 0)
  	DBUG_RETURN(HA_ERR_END_OF_FILE);
  else
  	rc = 0;
  memcpy(buf, key, get_key_len());
  MYSQL_INDEX_READ_ROW_DONE(rc);
  //rc= HA_ERR_WRONG_COMMAND;
  DBUG_RETURN(rc);
}


/**
  @brief
  index_last() asks for the last key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_spartan::index_last(uchar *buf)
{
  int rc;
  uchar *key=0;
  
  DBUG_ENTER("ha_spartan::index_last");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  key = share->index_class->get_last_key();
  if(key == 0)
  	DBUG_RETURN(HA_ERR_END_OF_FILE);
  else
  	rc = 0;
  memcpy(buf, key, get_key_len());
  MYSQL_INDEX_READ_ROW_DONE(rc);
  //rc= HA_ERR_WRONG_COMMAND;
  DBUG_RETURN(rc);
}


/**
  @brief
  rnd_init() is called when the system wants the storage engine to do a table
  scan. See the spartan in the introduction at the top of this file to see when
  rnd_init() is called.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_spartan::rnd_init(bool scan)
{
  DBUG_ENTER("ha_spartan::rnd_init");
  current_position = 0;
  stats.records = 0;
  ref_length = sizeof(long long);
  DBUG_RETURN(0);
}

int ha_spartan::rnd_end()
{
  DBUG_ENTER("ha_spartan::rnd_end");
  DBUG_RETURN(0);
}


/**
  @brief
  This is called for each row of the table scan. When you run out of records
  you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
  The Field structure for the table is the key to getting data into buf
  in a manner that will allow the server to understand it.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_spartan::rnd_next(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_spartan::rnd_next");
  //rc= HA_ERR_END_OF_FILE;
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);

  /*
	Read the row from the data file.
  */
  rc = share->data_class->read_row(buf, table->s->rec_buff_length, current_position);
  if(rc != -1)
  	current_position = (off_t)share->data_class->cur_position();
  else
  	DBUG_RETURN(HA_ERR_END_OF_FILE);
  stats.records++;
  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  position() is called after each call to rnd_next() if the data needs
  to be ordered. You can do something like the following to store
  the position:
  @code
  my_store_ptr(ref, ref_length, current_position);
  @endcode

  @details
  The server uses ref to store data. ref_length in the above case is
  the size needed to store current_position. ref is just a byte array
  that the server will maintain. If you are using offsets to mark rows, then
  current_position should be the offset. If it is a primary key like in
  BDB, then it needs to be a primary key.

  Called from filesort.cc, sql_select.cc, sql_delete.cc, and sql_update.cc.

  @see
  filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
*/
void ha_spartan::position(const uchar *record)
{
  DBUG_ENTER("ha_spartan::position");
  my_store_ptr(ref, ref_length, current_position);
  DBUG_VOID_RETURN;
}


/**
  @brief
  This is like rnd_next, but you are given a position to use
  to determine the row. The position will be of the type that you stored in
  ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
  or position you saved when position() was called.

  @details
  Called from filesort.cc, records.cc, sql_insert.cc, sql_select.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
*/
int ha_spartan::rnd_pos(uchar *buf, uchar *pos)
{
  int rc;
  DBUG_ENTER("ha_spartan::rnd_pos");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);
  ha_statistic_increment(&SSV::ha_read_rnd_next_count);
  current_position = (off_t)my_get_ptr(pos, ref_length);
  rc = share->data_class->read_row(buf, current_position, -1);
  MYSQL_READ_ROW_DONE(rc);
  //rc= HA_ERR_WRONG_COMMAND;
  DBUG_RETURN(rc);
}


/**
  @brief
  ::info() is used to return information to the optimizer. See my_base.h for
  the complete description.

  @details
  Currently this table handler doesn't implement most of the fields really needed.
  SHOW also makes use of this data.

  You will probably want to have the following in your code:
  @code
  if (records < 2)
    records = 2;
  @endcode
  The reason is that the server will optimize for cases of only a single
  record. If, in a table scan, you don't know the number of records, it
  will probably be better to set records to two so you can return as many
  records as you need. Along with records, a few more variables you may wish
  to set are:
    records
    deleted
    data_file_length
    index_file_length
    delete_length
    check_time
  Take a look at the public variables in handler.h for more information.

  Called in filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_table.cc, sql_union.cc, and sql_update.cc.

  @see
  filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc, sql_delete.cc,
  sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_table.cc,
  sql_union.cc and sql_update.cc
*/
int ha_spartan::info(uint flag)
{
  DBUG_ENTER("ha_spartan::info");
  /* This is a lie, but you don't want the optimizer to see zero or 1 */
  if(stats.records < 2)
  	stats.records = 2;
  DBUG_RETURN(0);
}


/**
  @brief
  extra() is called whenever the server wishes to send a hint to
  the storage engine. The myisam engine implements the most hints.
  ha_innodb.cc has the most exhaustive list of these hints.

    @see
  ha_innodb.cc
*/
int ha_spartan::extra(enum ha_extra_function operation)
{
  DBUG_ENTER("ha_spartan::extra");
  DBUG_RETURN(0);
}


/**
  @brief
  Used to delete all rows in a table, including cases of truncate and cases where
  the optimizer realizes that all rows will be removed as a result of an SQL statement.

  @details
  Called from item_sum.cc by Item_func_group_concat::clear(),
  Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
  Called from sql_delete.cc by mysql_delete().
  Called from sql_select.cc by JOIN::reinit().
  Called from sql_union.cc by st_select_lex_unit::exec().

  @see
  Item_func_group_concat::clear(), Item_sum_count_distinct::clear() and
  Item_func_group_concat::clear() in item_sum.cc;
  mysql_delete() in sql_delete.cc;
  JOIN::reinit() in sql_select.cc and
  st_select_lex_unit::exec() in sql_union.cc.
*/
int ha_spartan::delete_all_rows()
{
  DBUG_ENTER("ha_spartan::delete_all_rows");
  /*
	Begin critical section by locking the spartan mutex variable.
  */
  mysql_mutex_lock(&share->mutex);
  share->data_class->trunc_table();
  share->index_class->destroy_index();
  share->index_class->trunc_index();
  /*
	End section by unlocking the spartan mutex variable.
  */
  mysql_mutex_unlock(&share->mutex);
  DBUG_RETURN(0);
  //DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  This create a lock on the table. If you are implementing a storage engine
  that can handle transacations look at ha_berkely.cc to see how you will
  want to go about doing this. Otherwise you should consider calling flock()
  here. Hint: Read the section "locking functions for mysql" in lock.cc to understand
  this.

  @details
  Called from lock.cc by lock_external() and unlock_external(). Also called
  from sql_table.cc by copy_data_between_tables().

  @see
  lock.cc by lock_external() and unlock_external() in lock.cc;
  the section "locking functions for mysql" in lock.cc;
  copy_data_between_tables() in sql_table.cc.
*/
int ha_spartan::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_spartan::external_lock");
  DBUG_RETURN(0);
}


/**
  @brief
  The idea with handler::store_lock() is: The statement decides which locks
  should be needed for the table. For updates/deletes/inserts we get WRITE
  locks, for SELECT... we get read locks.

  @details
  Before adding the lock into the table lock handler (see thr_lock.c),
  mysqld calls store lock with the requested locks. Store lock can now
  modify a write lock to a read lock (or some other lock), ignore the
  lock (if we don't want to use MySQL table locks at all), or add locks
  for many tables (like we do when we are using a MERGE handler).

  Berkeley DB, for spartan, changes all WRITE locks to TL_WRITE_ALLOW_WRITE
  (which signals that we are doing WRITES, but are still allowing other
  readers and writers).

  When releasing locks, store_lock() is also called. In this case one
  usually doesn't have to do anything.

  In some exceptional cases MySQL may send a request for a TL_IGNORE;
  This means that we are requesting the same lock as last time and this
  should also be ignored. (This may happen when someone does a flush
  table when we have opened a part of the tables, in which case mysqld
  closes and reopens the tables and tries to get the same locks at last
  time). In the future we will probably try to remove this.

  Called from lock.cc by get_lock_data().

  @note
  In this method one should NEVER rely on table->in_use, it may, in fact,
  refer to a different thread! (this happens if get_lock_data() is called
  from mysql_lock_abort_for_thread() function)

  @see
  get_lock_data() in lock.cc
*/
THR_LOCK_DATA **ha_spartan::store_lock(THD *thd,
                                       THR_LOCK_DATA **to,
                                       enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type=lock_type;
  *to++= &lock;
  return to;
}


/**
  @brief
  Used to delete a table. By the time delete_table() has been called all
  opened references to this table will have been closed (and your globally
  shared references released). The variable name will just be the name of
  the table. You will need to remove any files you have created at this point.

  @details
  If you do not implement this, the default delete_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().

  Called from handler.cc by delete_table and ha_create_table(). Only used
  during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
  the storage engine.

  @see
  delete_table and ha_create_table() in handler.cc
*/
int ha_spartan::delete_table(const char *name)
{
  DBUG_ENTER("ha_spartan::delete_table");
  char name_buff[FN_REFLEN];

  /*
	Call the mysql delete file method.
	Note: the fn_format() method correctly creates a file name from the
	name passed into the method.
  */
  my_delete(fn_format(name_buff, name, "", SDE_EXT,
  					MY_REPLACE_EXT | MY_UNPACK_FILENAME), MYF(0));
  /*
	Call the mysql delete file method.
	Note: the fn_format() method correctly creates a file name from the
	name passed into the method.
  */
  my_delete(fn_format(name_buff, name, "", SDI_EXT,
  					MY_REPLACE_EXT | MY_UNPACK_FILENAME), MYF(0));
  /* This is not implemented but we want someone to be able that it works. */
  DBUG_RETURN(0);
}


/**
  @brief
  Given a starting key and an ending key, estimate the number of rows that
  will exist between the two keys.

  @details
  end_key may be empty, in which case determine if start_key matches any rows.

  Called from opt_range.cc by check_quick_keys().

  @see
  check_quick_keys() in opt_range.cc
*/
ha_rows ha_spartan::records_in_range(uint inx, key_range *min_key,
                                     key_range *max_key)
{
  DBUG_ENTER("ha_spartan::records_in_range");
  DBUG_RETURN(10);                         // low number to force index usage
}


/**
  @brief
  create() is called to create a database. The variable name will have the name
  of the table.

  @details
  When create() is called you do not need to worry about
  opening the table. Also, the .frm file will have already been
  created so adjusting create_info is not necessary. You can overwrite
  the .frm file at this point if you wish to change the table
  definition, but there are no methods currently provided for doing
  so.

  Called from handle.cc by ha_create_table().

  @see
  ha_create_table() in handle.cc
*/

int ha_spartan::create(const char *name, TABLE *table_arg,
                       HA_CREATE_INFO *create_info)
{
#if 0
#ifndef DBUG_OFF
  ha_table_option_struct *options= table_arg->s->option_struct;
  DBUG_ENTER("ha_spartan::create");
  /*
    This spartan shows how to support custom engine specific table and field
    options.
  */
  DBUG_ASSERT(options);
  DBUG_PRINT("info", ("strparam: '%-.64s'  ullparam: %llu  enumparam: %u  "\
                      "boolparam: %u",
                      (options->strparam ? options->strparam : "<NULL>"),
                      options->ullparam, options->enumparam, options->boolparam));
  for (Field **field= table_arg->s->field; *field; field++)
  {
    ha_field_option_struct *field_options= (*field)->option_struct;
    DBUG_ASSERT(field_options);
    DBUG_PRINT("info", ("field: %s  complex: '%-.64s'",
                         (*field)->field_name,
                         (field_options->complex_param_to_parse_it_in_engine ?
                          field_options->complex_param_to_parse_it_in_engine :
                          "<NULL>")));
  }
#endif
#endif
  DBUG_ENTER("ha_spartan::create");
  char name_buff[FN_REFLEN];
  char name_buff2[FN_REFLEN];

  if(!(share = get_share()))
  	DBUG_RETURN(1);

  /*
	Call the data class create table method.
	Note: the fn_format() method correctly creates a file name from the
	name passed into the method.
  */
  if(share->data_class->create_table(fn_format(name_buff, name, "", SDE_EXT,
  									MY_REPLACE_EXT | MY_UNPACK_FILENAME)))
  	DBUG_RETURN(-1);
  share->data_class->close_table();
  if(share->index_class->create_index(fn_format(name_buff2, name, "", SDI_EXT,
  									MY_REPLACE_EXT | MY_UNPACK_FILENAME), 128))
  	DBUG_RETURN(-1);
  share->index_class->close_index();
  DBUG_RETURN(0);
}


/**
  check_if_supported_inplace_alter() is used to ask the engine whether
  it can execute this ALTER TABLE statement in place or the server needs to
  create a new table and copy th data over.

  The engine may answer that the inplace alter is not supported or,
  if supported, whether the server should protect the table from concurrent
  accesses. Return values are

    HA_ALTER_INPLACE_NOT_SUPPORTED
    HA_ALTER_INPLACE_EXCLUSIVE_LOCK
    HA_ALTER_INPLACE_SHARED_LOCK
    etc
*/

enum_alter_inplace_result
ha_spartan::check_if_supported_inplace_alter(TABLE* altered_table,
                                             Alter_inplace_info* ha_alter_info)
{
  HA_CREATE_INFO *info= ha_alter_info->create_info;
  DBUG_ENTER("ha_spartan::check_if_supported_inplace_alter");

  if (ha_alter_info->handler_flags & Alter_inplace_info::CHANGE_CREATE_OPTION)
  {
    /*
      This spartan shows how custom engine specific table and field
      options can be accessed from this function to be compared.
    */
    ha_table_option_struct *param_new= info->option_struct;
    ha_table_option_struct *param_old= table->s->option_struct;

    /*
      check important parameters:
      for this spartan engine, we'll assume that changing ullparam or
      boolparam requires a table to be rebuilt, while changing strparam
      or enumparam - does not.

      For debugging purposes we'll announce this to the user
      (don't do it in production!)

    */
    if (param_new->ullparam != param_old->ullparam)
    {
      push_warning_printf(ha_thd(), Sql_condition::WARN_LEVEL_NOTE,
                          ER_UNKNOWN_ERROR, "SPARTAN DEBUG: ULL %llu -> %llu",
                          param_old->ullparam, param_new->ullparam);
      DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
    }

    if (param_new->boolparam != param_old->boolparam)
    {
      push_warning_printf(ha_thd(), Sql_condition::WARN_LEVEL_NOTE,
                          ER_UNKNOWN_ERROR, "SPARTAN DEBUG: YESNO %u -> %u",
                          param_old->boolparam, param_new->boolparam);
      DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
    }
  }

  if (ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_OPTION)
  {
    for (uint i= 0; i < table->s->fields; i++)
    {
      ha_field_option_struct *f_old= table->s->field[i]->option_struct;
      ha_field_option_struct *f_new= info->fields_option_struct[i];
      DBUG_ASSERT(f_old);
      if (f_new)
      {
        push_warning_printf(ha_thd(), Sql_condition::WARN_LEVEL_NOTE,
                            ER_UNKNOWN_ERROR, "SPARTAN DEBUG: Field %`s COMPLEX '%s' -> '%s'",
                            table->s->field[i]->field_name,
                            f_old->complex_param_to_parse_it_in_engine,
                            f_new->complex_param_to_parse_it_in_engine);
      }
      else
        DBUG_PRINT("info", ("old field %i did not changed", i));
    }
  }

  DBUG_RETURN(HA_ALTER_INPLACE_EXCLUSIVE_LOCK);
}


struct st_mysql_storage_engine spartan_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

static ulong srv_enum_var= 0;
static ulong srv_ulong_var= 0;
static double srv_double_var= 0;

const char *enum_var_names[]=
{
  "e1", "e2", NullS
};

TYPELIB enum_var_typelib=
{
  array_elements(enum_var_names) - 1, "enum_var_typelib",
  enum_var_names, NULL
};

static MYSQL_SYSVAR_ENUM(
  enum_var,                       // name
  srv_enum_var,                   // varname
  PLUGIN_VAR_RQCMDARG,            // opt
  "Sample ENUM system variable.", // comment
  NULL,                           // check
  NULL,                           // update
  0,                              // def
  &enum_var_typelib);             // typelib

static MYSQL_THDVAR_INT(int_var, PLUGIN_VAR_RQCMDARG, "-1..1",
  NULL, NULL, 0, -1, 1, 0);

static MYSQL_SYSVAR_ULONG(
  ulong_var,
  srv_ulong_var,
  PLUGIN_VAR_RQCMDARG,
  "0..1000",
  NULL,
  NULL,
  8,
  0,
  1000,
  0);

static MYSQL_SYSVAR_DOUBLE(
  double_var,
  srv_double_var,
  PLUGIN_VAR_RQCMDARG,
  "0.500000..1000.500000",
  NULL,
  NULL,
  8.5,
  0.5,
  1000.5,
  0);                             // reserved always 0

static MYSQL_THDVAR_DOUBLE(
  double_thdvar,
  PLUGIN_VAR_RQCMDARG,
  "0.500000..1000.500000",
  NULL,
  NULL,
  8.5,
  0.5,
  1000.5,
  0);

static struct st_mysql_sys_var* spartan_system_variables[]= {
  MYSQL_SYSVAR(enum_var),
  MYSQL_SYSVAR(ulong_var),
  MYSQL_SYSVAR(int_var),
  MYSQL_SYSVAR(double_var),
  MYSQL_SYSVAR(double_thdvar),
  MYSQL_SYSVAR(varopt_default),
  NULL
};

// this is an spartan of SHOW_SIMPLE_FUNC and of my_snprintf() service
// If this function would return an array, one should use SHOW_FUNC
static int show_func_spartan(MYSQL_THD thd, struct st_mysql_show_var *var,
                             char *buf)
{
  var->type= SHOW_CHAR;
  var->value= buf; // it's of SHOW_VAR_FUNC_BUFF_SIZE bytes
  my_snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE,
              "enum_var is %lu, ulong_var is %lu, int_var is %d, "
              "double_var is %f, %.6b", // %b is a MySQL extension
              srv_enum_var, srv_ulong_var, THDVAR(thd, int_var),
              srv_double_var, "really");
  return 0;
}

static struct st_mysql_show_var func_status[]=
{
  {"func_spartan",  (char *)show_func_spartan, SHOW_SIMPLE_FUNC},
  {0,0,SHOW_UNDEF}
};

struct st_mysql_daemon unusable_spartan=
{ MYSQL_DAEMON_INTERFACE_VERSION };

mysql_declare_plugin(spartan)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &spartan_storage_engine,
  "SPARTAN",
  "Brian Aker, MySQL AB",
  "Spartan storage engine",
  PLUGIN_LICENSE_GPL,
  spartan_init_func,                            /* Plugin Init */
  NULL,                                         /* Plugin Deinit */
  0x0001 /* 0.1 */,
  func_status,                                  /* status variables */
  spartan_system_variables,                     /* system variables */
  NULL,                                         /* config options */
  0,                                            /* flags */
}
mysql_declare_plugin_end;
maria_declare_plugin(spartan)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &spartan_storage_engine,
  "SPARTAN",
  "Brian Aker, MySQL AB",
  "Spartan storage engine",
  PLUGIN_LICENSE_GPL,
  spartan_init_func,                            /* Plugin Init */
  NULL,                                         /* Plugin Deinit */
  0x0001,                                       /* version number (0.1) */
  func_status,                                  /* status variables */
  spartan_system_variables,                     /* system variables */
  "0.1",                                        /* string version */
  MariaDB_PLUGIN_MATURITY_EXPERIMENTAL          /* maturity */
},
{
  MYSQL_DAEMON_PLUGIN,
  &unusable_spartan,
  "UNUSABLE",
  "Sergei Golubchik",
  "Unusable Daemon",
  PLUGIN_LICENSE_GPL,
  NULL,                                         /* Plugin Init */
  NULL,                                         /* Plugin Deinit */
  0x030E,                                       /* version number (3.14) */
  NULL,                                         /* status variables */
  NULL,                                         /* system variables */
  "3.14.15.926" ,                               /* version, as a string */
  MariaDB_PLUGIN_MATURITY_EXPERIMENTAL          /* maturity */
}
maria_declare_plugin_end;
