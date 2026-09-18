package org.akhq.employee.security;

import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.rules.AbstractSecurityRule;
import io.micronaut.security.rules.SecuredAnnotationRule;
import io.micronaut.security.rules.SecurityRuleResult;
import io.micronaut.security.token.RolesFinder;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.akhq.employee.repository.EmployeeRepository;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * The JWT's admin/super_admin claims are fixed at login time, so a plain @Secured("ROLE_ADMIN")
 * check on /api/admin/** would keep trusting a token whose employee was demoted or deactivated
 * minutes ago, until the token expires. This rule re-checks live DB state on every admin request,
 * the same way EmployeeGrantSecurityRule does for AKHQSecured routes.
 */
@Singleton
public class AdminAccessSecurityRule extends AbstractSecurityRule<HttpRequest<?>> {
    public static final String ADMIN_PATH_PREFIX = "/api/admin";

    public AdminAccessSecurityRule(RolesFinder rolesFinder) {
        super(rolesFinder);
    }

    @Inject
    private EmployeeRepository employeeRepository;

    @Override
    public Publisher<SecurityRuleResult> check(HttpRequest<?> request, Authentication authentication) {
        if (!request.getPath().startsWith(ADMIN_PATH_PREFIX)) {
            return Mono.just(SecurityRuleResult.UNKNOWN);
        }
        if (authentication == null
            || !EmployeeAuthenticationProvider.AUTH_SOURCE.equals(authentication.getAttributes().get("auth_source"))) {
            return Mono.just(SecurityRuleResult.UNKNOWN);
        }

        String employeeCode = String.valueOf(authentication.getAttributes().get("employee_code"));

        return Mono.fromCallable(() -> employeeRepository.findByEmployeeCode(employeeCode))
            .subscribeOn(Schedulers.boundedElastic())
            .map(optionalEmployee -> {
                boolean stillAdmin = optionalEmployee.isPresent()
                    && optionalEmployee.get().isActive()
                    && optionalEmployee.get().isAdmin();
                return stillAdmin ? SecurityRuleResult.UNKNOWN : SecurityRuleResult.REJECTED;
            });
    }

    public static final Integer ORDER = SecuredAnnotationRule.ORDER - 100;

    public int getOrder() {
        return ORDER;
    }
}
