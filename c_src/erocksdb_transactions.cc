// -------------------------------------------------------------------
// Copyright (c) 2016 Benoit Chesneau. All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// -------------------------------------------------------------------


#include <vector>
#include <memory>

#include "erocksdb.h"

#include "rocksdb/db.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/transaction_log.h"


#ifndef INCL_REFOBJECTS_H
#include "refobjects.h"
#endif

#ifndef ATOMS_H
#include "atoms.h"
#endif

#ifndef INCL_UTIL_H
#include "util.h"
#endif

namespace erocksdb {


ERL_NIF_TERM
GetUpdatesSince(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{

    ErlNifSInt64 seq;
    std::unique_ptr<rocksdb::TransactionLogIterator> iter;
    ReferencePtr<DbObject> db_ptr;

    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    if(!enif_get_int64(env, argv[1], &seq))
        return enif_make_badarg(env);


    rocksdb::Status status = db_ptr->m_Db->GetUpdatesSince((uint64_t)seq, &iter);

    if(!status.ok())
        return error_tuple(env, ATOM_ERROR, status);

    TransactionLogObject * transaction_log_ptr;
    transaction_log_ptr = TransactionLogObject::CreateTransactionLogObject(db_ptr.get(), iter.release());
    ERL_NIF_TERM result = enif_make_resource(env, transaction_log_ptr);
    enif_release_resource(transaction_log_ptr);
    return enif_make_tuple2(env, ATOM_OK, result);
}

ERL_NIF_TERM
TransactionLogIteratorNext(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    TransactionLogObject * transaction_log_ptr;
    transaction_log_ptr=TransactionLogObject::RetrieveTransactionLogObject(env, argv[0], true);
    if(NULL==transaction_log_ptr)
        return enif_make_badarg(env);

    auto itr = transaction_log_ptr->m_TransactionLogIterator;

    if(!itr->Valid())
        return enif_make_tuple2(env, ATOM_ERROR, ATOM_INVALID_ITERATOR);

    rocksdb::BatchResult batch = itr->GetBatch();
    rocksdb::Status status = itr->status();
    if (!status.ok()) {
        return error_tuple(env, ATOM_ERROR, status);
    }

    std::string batch_str = batch.writeBatchPtr->Data();
    return enif_make_tuple3(env,
                            ATOM_OK,
                            enif_make_int64(env, batch.sequence),
                            enif_make_string(env, batch_str.c_str(), ERL_NIF_LATIN1));
}


ERL_NIF_TERM
TransactionLogIteratorClose(
        ErlNifEnv* env,
        int argc,
        const ERL_NIF_TERM argv[])
{
    TransactionLogObject * transaction_log_ptr;
    transaction_log_ptr=TransactionLogObject::RetrieveTransactionLogObject(env, argv[0], true);
    if(NULL!=transaction_log_ptr)
    {
        ErlRefObject::InitiateCloseRequest(transaction_log_ptr);
        transaction_log_ptr = NULL;
        return ATOM_OK;
    }
    return enif_make_badarg(env);
}

}