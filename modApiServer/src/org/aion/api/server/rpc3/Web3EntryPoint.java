package org.aion.api.server.rpc3;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.aion.log.AionLoggerFactory;
import org.aion.log.LogEnum;
import org.aion.rpc.errors.RPCExceptions.InternalErrorRPCException;
import org.aion.rpc.errors.RPCExceptions.InvalidRequestRPCException;
import org.aion.rpc.errors.RPCExceptions.RPCException;
import org.aion.rpc.server.OpsRPC;
import org.aion.rpc.server.PersonalRPC;
import org.aion.rpc.server.RPC;
import org.aion.rpc.server.StratumRPC;
import org.aion.rpc.types.RPCTypes.RPCError;
import org.aion.rpc.types.RPCTypes.Request;
import org.aion.rpc.types.RPCTypes.Response;
import org.aion.rpc.types.RPCTypes.VersionType;
import org.aion.rpc.types.RPCTypesConverter.RequestConverter;
import org.aion.rpc.types.RPCTypesConverter.ResponseConverter;
import org.slf4j.Logger;

public class Web3EntryPoint {

    private final Set<String> enabledMethods;
    private final Set<String> disabledMethods;
    private Map<String, RPC> rpcMap;
    private static final Logger logger = AionLoggerFactory.getLogger(LogEnum.API.name());
    //TODO this map should eventually be removed
    //TODO this is currently used only to allow support of existing tooling
    private static final Map<String,String> methodInterfaceMap;
    static {
        methodInterfaceMap = Map
            .ofEntries(Map.entry("getseed", "stratum_getSeed"),
                Map.entry("submitseed", "stratum_submitSeed"),
                Map.entry("submitsignature", "stratum_submitSignature"));
    }

    public Web3EntryPoint(PersonalRPC personal, OpsRPC ops,
        StratumRPC stratum, List<String> enabledGroup, List<String> enabledMethods,
        List<String> disabledMethods){
        this.enabledMethods = Set.copyOf(enabledMethods);
        this.disabledMethods = Set.copyOf(disabledMethods);
        Map<String, RPC> temp = new HashMap<>();
        temp.put("personal", personal);
        temp.put("ops", ops);
        temp.put("stratum", stratum);
        if (enabledGroup != null) {
            for (String s: Set.copyOf(temp.keySet())){
                if (!enabledGroup.contains(s)){
                    temp.remove(s);
                }
            }
        }
        rpcMap = Collections.unmodifiableMap(temp);
    }

    public String call(String requestString){
        logger.debug("Received request: {}",requestString);
        Request request = null;
        RPCError err;
        Integer id = null;
        try{
            request = readRequest(requestString);

            id = request.id;
            String group = request.method.split("_")[0];
            if (rpcMap.containsKey(group) &&
                checkMethod(request.method)){
                final String response = ResponseConverter.encodeStr(
                    new Response(request.id, rpcMap.get(group).execute(request), null,
                        VersionType.Version2));
                logger.debug("Response: {}", response);
                return response;
            }else {
                logger.debug(
                        "Request attempted to call a method on a disabled interface: {}",
                        request.method);
                err= InvalidRequestRPCException.INSTANCE.getError();
            }
        }catch (InvalidRequestRPCException e){
            err = e.getError();//Don't log this error since it may already be logged elsewhere
        }
        catch (RPCException e){
            logger.debug("Request failed due to an RPC exception: ", e);
            err = e.getError();
        }
        catch (Exception e){
            logger.debug("Call to {} failed.", request==null? "null":request.method);
            logger.debug("Request failed due to an internal error: ", e);
            err= InternalErrorRPCException.INSTANCE.getError();
        }
        return ResponseConverter.encodeStr(new Response(id, null, err, VersionType.Version2));
    }

    private static Request readRequest(String requestString) {
        Request request;
        try{
            request = readAndEnforceInterface(requestString);
        }catch (Exception e){
            logger.debug("Received an invalid request: {}", requestString);
            throw InvalidRequestRPCException.INSTANCE;
        }
        if (request==null) throw InvalidRequestRPCException.INSTANCE;
        return request;
    }

    /**
     * Reads the request and binds getseed, submitseed, and submitsignature to stratum
     * @param requestString the client request
     * @return null if the request string is null or a valid request
     */
    private static Request readAndEnforceInterface(String requestString) {
        Request request = RequestConverter.decode(requestString);
        if (request == null) {
            return null;//
        } else if (methodInterfaceMap.containsKey(request.method)){
            return new Request(request.id,
                methodInterfaceMap.get(request.method),
                request.params,
                request.jsonrpc);
        } else {
            return request;
        }
    }

    public boolean isExecutable(String method){
        if (methodInterfaceMap.containsKey(method)){
            method=methodInterfaceMap.get(method);// add the interface to this method
        }
        String group = method.split("_")[0];
        return rpcMap.containsKey(group) && rpcMap.get(group).isExecutable(method);
    }

    public boolean checkMethod(String method){
       if (enabledMethods !=null && enabledMethods.contains(method)){
           return true;
       }
       else
           return disabledMethods == null || !disabledMethods.contains(method);
    }
}
