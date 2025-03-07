import java.util.*;

public class HostPortTypeValidator {
    public static void main(String[] args) {
        // Map 1: Hostname -> List of ports
        Map<String, List<Integer>> hostPortMap = new HashMap<>();
        hostPortMap.put("hostA", Arrays.asList(8081, 8082));
        hostPortMap.put("hostB", Arrays.asList(9090));
        hostPortMap.put("hostC", Arrays.asList(7070));

        // Map 2: Hostname -> Host type
        Map<String, String> hostTypeMap = new HashMap<>();
        hostTypeMap.put("hostA", "Type1");
        hostTypeMap.put("hostB", "Type2");
        // Note: "hostC" is missing from hostTypeMap, so it's invalid.

        // Validate hostPortMap using hostTypeMap
        validateHostPortMap(hostPortMap, hostTypeMap);
    }

    /**
     * Validates if the entries in hostPortMap have corresponding valid host types in hostTypeMap.
     */
    private static void validateHostPortMap(Map<String, List<Integer>> hostPortMap, Map<String, String> hostTypeMap) {
        for (Map.Entry<String, List<Integer>> entry : hostPortMap.entrySet()) {
            String host = entry.getKey();
            List<Integer> ports = entry.getValue();

            // Check if the host exists in hostTypeMap
            if (hostTypeMap.containsKey(host)) {
                String hostType = hostTypeMap.get(host);
                System.out.println("Host: " + host + " with ports " + ports + " is valid. Host type: " + hostType);
            } else {
                System.out.println("Host: " + host + " with ports " + ports + " is invalid. No matching host type found.");
            }
        }
    }
}


import java.util.*;

public class HostPortValidator {
    public static void main(String[] args) {
        // Map 1: Hostname -> List of ports
        Map<String, List<Integer>> hostPortMap = new HashMap<>();
        hostPortMap.put("hostA", Arrays.asList(8081, 8082));
        hostPortMap.put("hostB", Arrays.asList(9090));
        hostPortMap.put("hostC", Arrays.asList(7070));

        // Map 2: Hostname -> List of valid ports
        Map<String, List<Integer>> hostPortValidationMap = new HashMap<>();
        hostPortValidationMap.put("hostA", Arrays.asList(8081, 8083)); // Note: 8082 is missing
        hostPortValidationMap.put("hostB", Arrays.asList(9090));
        // "hostC" is missing from this map, so it will be marked invalid.

        // Validate hostPortMap using hostPortValidationMap
        validateHostPortMap(hostPortMap, hostPortValidationMap);
    }

    /**
     * Validates if the entries in hostPortMap have corresponding valid hostname and port in hostPortValidationMap.
     */
    private static void validateHostPortMap(Map<String, List<Integer>> hostPortMap, Map<String, List<Integer>> hostPortValidationMap) {
        for (Map.Entry<String, List<Integer>> entry : hostPortMap.entrySet()) {
            String host = entry.getKey();
            List<Integer> ports = entry.getValue();

            // Check if the host exists in the validation map
            if (hostPortValidationMap.containsKey(host)) {
                List<Integer> validPorts = hostPortValidationMap.get(host);
                for (int port : ports) {
                    if (validPorts.contains(port)) {
                        System.out.println("Host: " + host + " with port " + port + " is valid.");
                    } else {
                        System.out.println("Host: " + host + " with port " + port + " is invalid. No matching valid port found.");
                    }
                }
            } else {
                System.out.println("Host: " + host + " with ports " + ports + " is invalid. Host not found in validation map.");
            }
        }
    }
}
