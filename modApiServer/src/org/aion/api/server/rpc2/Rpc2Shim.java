package org.aion.api.server.rpc2;

import org.aion.api.server.rpc2.autogen.RpcProcessor2;

import java.util.List;

/**
 * This is a class to help migrate to the "new" RPC framework (autogenerated
 * code for: deserializing input json; Java interface representing the interface
 * of the JSON RPC server; serialization of the outputs from the interface methods
 * back to JSON).
 *
 * Any "shim" code to make the new RPC framework temporarily live within the existing
 * one should live here (so we can transition methods incrementally instead of in
 * one huge change).  Conversely, avoid putting in code here that's for any purpose
 * other than adapting the new RPC to the old one -- this class will be removed when
 * the transition is complete.
 */
public class Rpc2Shim {
    private AbstractRpcProcessor rpc = new RpcProcessor2(new RpcImpl());

    public static final List<String> SUPPORTED_METHOD_NAMES = List.of(
        "getseed", "submitseed", "submitwork", "eth_getTransactionByHash2", "eth_call2"
    );

    public static boolean supportsMethod(String methodName) {
        return SUPPORTED_METHOD_NAMES.contains(methodName);
    }

    public String process(String payload) {
        try {
            return rpc.process(payload);
        } catch (Exception ex) {
            // error cases not incorporated into autogen code / schema yet.
            // this is a very crude temporary placeholder.
            String err = ex.toString().replace("\n", "|").replace("\"", "\\\"");

            return "{\"jsonrpc\": \"2.0\", "
                + "\"error\": {\"code\": -32703, \"message\": \"Internal error:" + err +" \"},"
                + " \"id\": null}";
        }
    }
}
