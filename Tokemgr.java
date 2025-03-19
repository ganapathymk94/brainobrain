@Component
public class TokenManager {

    private final Map<String, LocalDateTime> tokenStore = new HashMap<>();

    public String generateToken() {
        String token = UUID.randomUUID().toString();
        tokenStore.put(token, LocalDateTime.now().plusHours(48)); // Token expires in 48 hours
        return token;
    }

    public boolean validateToken(String token) {
        LocalDateTime expiryTime = tokenStore.get(token);
        if (expiryTime != null && expiryTime.isAfter(LocalDateTime.now())) {
            tokenStore.remove(token); // Invalidate token after use
            return true;
        }
        return false; // Token expired or not found
    }
}
