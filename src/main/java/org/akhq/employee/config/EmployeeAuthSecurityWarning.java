package org.akhq.employee.config;

import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Value;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.lang.Runnable;

/**
 * The employee login flow authenticates on employee code alone - the password field is never
 * checked, by design, because the real corporate directory has no password to check. That makes
 * two things load-bearing for safety, and both are easy to leave misconfigured: mock-enabled must
 * be false before any real deployment, and this whole login path must sit behind a boundary
 * (VPN, reverse-proxy SSO, network ACL) that already authenticated the caller - it is not a
 * safe standalone credential check against the open internet.
 */
@Slf4j
@Context
public class EmployeeAuthSecurityWarning {
    @Value("${micronaut.security.enabled:false}")
    protected Boolean securityEnabled;

    @Inject
    protected EmployeeDirectoryProperties properties;

    @PostConstruct
    public void start() {
        if (!Boolean.TRUE.equals(securityEnabled)) {
            return;
        }

        if (properties.isMockEnabled() && !properties.getBootstrapAdminCodes().isEmpty()) {
            logWarning(() -> {
                log.warn("akhq.employee-directory.mock-enabled is true AND bootstrap-admin-codes is set.");
                log.warn("Any string typed as the employee code on the login form will authenticate,");
                log.warn("and a code matching bootstrap-admin-codes becomes a super admin with full access instantly.");
                log.warn("Set 'akhq.employee-directory.mock-enabled: false' and point 'base-url' at the real directory before this is reachable by anyone untrusted.");
            });
        } else if (properties.isMockEnabled()) {
            logWarning(() -> {
                log.warn("akhq.employee-directory.mock-enabled is true.");
                log.warn("Any non-blank employee code will authenticate successfully - there is no real directory check.");
                log.warn("Set 'akhq.employee-directory.mock-enabled: false' and configure 'base-url' before any real deployment.");
            });
        }

        log.warn("The employee login flow never checks a password - only the employee code, verified against the directory API.");
        log.warn("Do not expose this application directly to an untrusted network; put it behind a boundary that already authenticated the caller.");
    }

    private static void logWarning(Runnable printBody) {
        log.warn("");
        log.warn("##############################################################");
        log.warn("#             EMPLOYEE AUTH SECURITY WARNING                #");
        log.warn("##############################################################");
        log.warn("");
        printBody.run();
        log.warn("");
        log.warn("##############################################################");
        log.warn("");
    }
}
