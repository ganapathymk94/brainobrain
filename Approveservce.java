@Service
public class ApprovalService {

    private final EmailSender emailSender;
    private final TokenManager tokenManager;

    public ApprovalService(EmailSender emailSender, TokenManager tokenManager) {
        this.emailSender = emailSender;
        this.tokenManager = tokenManager;
    }

    public void sendApprovalEmail() {
        String token = tokenManager.generateToken(); // Generate a token with expiration
        String approvalLink = "http://localhost:8080/api/approvals/approve?token=" + token;
        
        // Send email
        String emailBody = "Please approve the request using this link (valid for 48 hours): " + approvalLink;
        emailSender.sendEmail("approver@example.com", "Approval Request", emailBody);
    }

    public boolean processApproval(String token) {
        return tokenManager.validateToken(token); // Check if token is valid and not expired
    }
}
