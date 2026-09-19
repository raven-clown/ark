package org.akhq.security.mapper;

import io.micronaut.context.annotation.Replaces;
import io.micronaut.core.convert.value.ConvertibleValues;
import io.micronaut.security.authentication.AuthenticationFailed;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.micronaut.security.ldap.ContextAuthenticationMapper;
import io.micronaut.security.ldap.DefaultContextAuthenticationMapper;
import io.micronaut.security.rules.SecurityRule;
import lombok.extern.slf4j.Slf4j;
import org.akhq.employee.domain.AccountType;
import org.akhq.employee.service.EmployeeAuthenticationService;
import org.akhq.models.security.ClaimRequest;
import org.akhq.models.security.ClaimResponse;
import org.akhq.models.security.ClaimProvider;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.akhq.models.security.ClaimProviderType;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Singleton
@Replaces(DefaultContextAuthenticationMapper.class)
public class LdapContextAuthenticationMapper implements ContextAuthenticationMapper {
    @Inject
    private ClaimProvider claimProvider;

    @Inject
    private EmployeeAuthenticationService employeeAuthenticationService;

    @Override
    public AuthenticationResponse map(ConvertibleValues<Object> attributes, String username, Set<String> groups) {
        try {
            employeeAuthenticationService.syncExternalIdentity(username, username, AccountType.LDAP);
        } catch (Exception e) {
            log.warn("Could not sync LDAP identity {} into the employee directory", username, e);
        }

        ClaimRequest request = ClaimRequest.builder()
                .providerType(ClaimProviderType.LDAP)
                .providerName(null)
                .username(username)
                .groups(List.copyOf(groups))
                .build();
        try {
            ClaimResponse claim = claimProvider.generateClaim(request);
            return AuthenticationResponse.success(username, List.of(SecurityRule.IS_AUTHENTICATED), Map.of("groups", claim.getGroups()));
        } catch (Exception e) {
            String claimProviderClass = claimProvider.getClass().getName();
            return new AuthenticationFailed("Exception from ClaimProvider " + claimProviderClass + ": " + e.getMessage());
        }
    }
}
