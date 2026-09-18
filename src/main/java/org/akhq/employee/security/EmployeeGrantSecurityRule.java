package org.akhq.employee.security;

import io.micronaut.http.BasicHttpAttributes;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.rules.AbstractSecurityRule;
import io.micronaut.security.rules.SecuredAnnotationRule;
import io.micronaut.security.rules.SecurityRuleResult;
import io.micronaut.security.token.RolesFinder;
import io.micronaut.web.router.MethodBasedRouteMatch;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.akhq.configs.security.Role;
import org.akhq.employee.repository.EmployeeRepository;
import org.akhq.employee.service.PermissionResolutionService;
import org.akhq.security.annotation.AKHQSecured;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Authorizes AKHQSecured routes for employees authenticated through the central directory,
 * resolving grants live from the RBAC database instead of the static akhq.security YAML.
 * Only takes effect when the authentication comes from EmployeeAuthenticationProvider,
 * otherwise defers (UNKNOWN) to the legacy AKHQSecurityRule.
 */
@Slf4j
@Singleton
public class EmployeeGrantSecurityRule extends AbstractSecurityRule<HttpRequest<?>> {

    public EmployeeGrantSecurityRule(RolesFinder rolesFinder) {
        super(rolesFinder);
    }

    @Inject
    private EmployeeRepository employeeRepository;
    @Inject
    private PermissionResolutionService permissionResolutionService;

    @Override
    public Publisher<SecurityRuleResult> check(HttpRequest<?> request, Authentication authentication) {
        if (authentication == null
            || !EmployeeAuthenticationProvider.AUTH_SOURCE.equals(authentication.getAttributes().get("auth_source"))) {
            return Mono.just(SecurityRuleResult.UNKNOWN);
        }

        var routeMatchInfo = BasicHttpAttributes.getRouteMatchInfo(request);
        if (routeMatchInfo.isEmpty() || !(routeMatchInfo.get() instanceof MethodBasedRouteMatch<?, ?> methodRoute)) {
            return Mono.just(SecurityRuleResult.UNKNOWN);
        }

        if (!methodRoute.hasAnnotation(AKHQSecured.class)) {
            return Mono.just(SecurityRuleResult.UNKNOWN);
        }

        Optional<Role.Resource> optionalResource = methodRoute.getValue(AKHQSecured.class, "resource", Role.Resource.class);
        Optional<Role.Action> optionalAction = methodRoute.getValue(AKHQSecured.class, "action", Role.Action.class);
        if (optionalResource.isEmpty() || optionalAction.isEmpty()) {
            return Mono.just(SecurityRuleResult.UNKNOWN);
        }

        if (!methodRoute.getVariableValues().containsKey("cluster")) {
            log.warn("Route matched AKHQSecured but no `cluster` provided");
            return Mono.just(SecurityRuleResult.REJECTED);
        }
        String cluster = methodRoute.getVariableValues().get("cluster").toString();

        String employeeCode = String.valueOf(authentication.getAttributes().get("employee_code"));

        return Mono.fromCallable(() -> employeeRepository.findByEmployeeCode(employeeCode))
            .subscribeOn(Schedulers.boundedElastic())
            .map(optionalEmployee -> {
                if (optionalEmployee.isEmpty() || !optionalEmployee.get().isActive()) {
                    return SecurityRuleResult.REJECTED;
                }
                boolean allowed = permissionResolutionService.resolveEffectiveGrants(optionalEmployee.get().getId())
                    .stream()
                    .anyMatch(grant -> grant.resource() == optionalResource.get()
                        && grant.action() == optionalAction.get()
                        && safeMatches(grant.clusterPattern(), cluster));
                return allowed ? SecurityRuleResult.ALLOWED : SecurityRuleResult.REJECTED;
            });
    }

    private boolean safeMatches(String pattern, String value) {
        try {
            return Pattern.matches(pattern, value);
        } catch (PatternSyntaxException e) {
            return false;
        }
    }

    public static final Integer ORDER = SecuredAnnotationRule.ORDER - 100;

    public int getOrder() {
        return ORDER;
    }
}
