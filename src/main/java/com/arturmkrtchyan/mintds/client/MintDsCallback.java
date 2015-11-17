package com.arturmkrtchyan.mintds.client;

public interface MintDsCallback {

    void onFailure(Throwable cause);

    void onSuccess(String msg);

}
