package org.apache.cassandra.auth;

import com.datastax.bdp.db.upgrade.SchemaUpgrade;
import com.datastax.bdp.db.upgrade.VersionDependentFeature;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.cassandra.auth.enums.PartitionedEnum;
import org.apache.cassandra.auth.permission.Permissions;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraAuthorizer implements IAuthorizer {
    private static final Logger logger = LoggerFactory.getLogger(CassandraAuthorizer.class);
    private static final String ROLE = "role";
    private static final String RESOURCE = "resource";
    private static final String PERMISSIONS = "permissions";
    private static final String GRANTABLES = "grantables";
    private static final String RESTRICTED = "restricted";
    private static final String ROLE_PERMISSIONS_TABLE = "system_auth.role_permissions";
    private final VersionDependentFeature<CassandraAuthorizer.VersionDepentent> feature;

    public CassandraAuthorizer() {
        this.feature = VersionDependentFeature.createForSchemaUpgrade("CassandraAuthorizer", new CassandraVersion("4.0"), new CassandraAuthorizer.Legacy(), new CassandraAuthorizer.Native(), new SchemaUpgrade(AuthKeyspace.metadata(), AuthKeyspace.tablesIfNotExist(), false), logger, "All live nodes are running DSE 6.0 or newer - preparing to use GRANT AUHTORIZE FOR and RESTRICT", "All live nodes are running DSE 6.0 or newer - GRANT AUHTORIZE FOR and RESTRICT available", "Not all live nodes are running DSE 6.0 or newer or upgrade in progress - GRANT AUHTORIZE FOR and RESTRICT are not available until all nodes are running DSE 6.0 or newer and automatic schema upgrade has finished");
    }

    public void revokeAllFrom(RoleResource revokee) {
        try {
            process("DELETE FROM system_auth.role_permissions WHERE role = '%s'", new Object[]{escape(revokee.getRoleName())});
        } catch (RequestValidationException | RequestExecutionException var3) {
            logger.warn("CassandraAuthorizer failed to revoke all permissions of {}: {}", revokee.getRoleName(), var3.getMessage());
        }

    }

    public Set<RoleResource> revokeAllOn(IResource droppedResource) {
        try {
            Set<String> roles = this.fetchRolesWithPermissionsOn(droppedResource);
            if (roles.isEmpty()) {
                return Collections.emptySet();
            } else {
                this.deletePermissionsFor(droppedResource, roles);
                return (Set) roles.stream().map(RoleResource::role).collect(Collectors.toSet());
            }
        } catch (RequestValidationException | RequestExecutionException var3) {
            logger.warn("CassandraAuthorizer failed to revoke all permissions on {}: {}", droppedResource, var3.getMessage());
            return Collections.emptySet();
        }
    }

    private void deletePermissionsFor(IResource resource, Set<String> roles) {
        process("DELETE FROM system_auth.role_permissions WHERE role IN (%s) AND resource = '%s'", new Object[]{roles.stream().map(CassandraAuthorizer::escape).collect(Collectors.joining("', '", "'", "'")), escape(resource.getName())});
    }

    private Set<String> fetchRolesWithPermissionsOn(IResource resource) {
        UntypedResultSet rows = process("SELECT role FROM system_auth.role_permissions WHERE resource = '%s' ALLOW FILTERING", new Object[]{escape(resource.getName())});
        Set<String> roles = new HashSet(rows.size());
        Iterator var4 = rows.iterator();

        while (var4.hasNext()) {
            UntypedResultSet.Row row = (UntypedResultSet.Row) var4.next();
            roles.add(row.getString("role"));
        }

        return roles;
    }

    public Map<IResource, PermissionSets> allPermissionSets(RoleResource role) {
        try {
            return this.permissionsForRole(role);
        } catch (RequestValidationException var3) {
            throw new AssertionError(var3);
        } catch (RequestExecutionException var4) {
            logger.warn("CassandraAuthorizer failed to authorize {}", role);
            throw new RuntimeException(var4);
        }
    }

    public Map<RoleResource, Map<IResource, PermissionSets>> allPermissionSetsMany(Iterable<? extends RoleResource> roles) {
        try {
            return this.permissionsForRoles(roles);
        } catch (RequestValidationException var3) {
            throw new AssertionError(var3);
        } catch (RequestExecutionException var4) {
            logger.warn("CassandraAuthorizer failed to authorize {}", roles);
            throw new RuntimeException(var4);
        }
    }

    private Map<IResource, PermissionSets> permissionsForRole(RoleResource role) {
        return ((CassandraAuthorizer.VersionDepentent) this.feature.implementation()).permissionsForRole(role);
    }

    private Map<RoleResource, Map<IResource, PermissionSets>> permissionsForRoles(Iterable<? extends RoleResource> roles) {
        return ((CassandraAuthorizer.VersionDepentent) this.feature.implementation()).permissionsForRoles(roles);
    }

    private static void permissionsFromRow(UntypedResultSet.Row row, String column, Consumer<Permission> perms) {
        if (row.has(column)) {
            row.getSet(column, UTF8Type.instance).stream().map(Permissions::permission).forEach(perms);
        }
    }

    public Set<Permission> grant(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource grantee, GrantMode... grantModes) {
        if (ArrayUtils.isEmpty(grantModes)) {
            throw new IllegalArgumentException("Must specify at least one grantMode");
        } else {
            String roleName = escape(grantee.getRoleName());
            String resourceName = escape(resource.getName());
            CassandraAuthorizer.VersionDepentent impl = (CassandraAuthorizer.VersionDepentent) this.feature.implementation();
            Set<Permission> grantedPermissions = Sets.newHashSetWithExpectedSize(permissions.size());
            GrantMode[] var10 = grantModes;
            int var11 = grantModes.length;

            for (int var12 = 0; var12 < var11; ++var12) {
                GrantMode grantMode = var10[var12];
                String grantModeColumn = impl.columnForGrantMode(grantMode);
                Set<Permission> nonExistingPermissions = new HashSet(permissions);
                nonExistingPermissions.removeAll(this.getExistingPermissions(roleName, resourceName, grantModeColumn, permissions));
                if (!nonExistingPermissions.isEmpty()) {
                    String perms = (String) nonExistingPermissions.stream().map(PartitionedEnum::getFullName).collect(Collectors.joining("','", "'", "'"));
                    this.updatePermissions(roleName, resourceName, grantModeColumn, "+", perms);
                    grantedPermissions.addAll(nonExistingPermissions);
                }
            }

            return grantedPermissions;
        }
    }

    public Set<Permission> revoke(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource revokee, GrantMode... grantModes) {
        if (ArrayUtils.isEmpty(grantModes)) {
            throw new IllegalArgumentException("Must specify at least one grantMode");
        } else {
            String roleName = escape(revokee.getRoleName());
            String resourceName = escape(resource.getName());
            CassandraAuthorizer.VersionDepentent impl = (CassandraAuthorizer.VersionDepentent) this.feature.implementation();
            Set<Permission> revokedPermissions = Sets.newHashSetWithExpectedSize(permissions.size());
            GrantMode[] var10 = grantModes;
            int var11 = grantModes.length;

            for (int var12 = 0; var12 < var11; ++var12) {
                GrantMode grantMode = var10[var12];
                String grantModeColumn = impl.columnForGrantMode(grantMode);
                Set<Permission> existingPermissions = this.getExistingPermissions(roleName, resourceName, grantModeColumn, permissions);
                if (!existingPermissions.isEmpty()) {
                    String perms = (String) existingPermissions.stream().map(PartitionedEnum::getFullName).collect(Collectors.joining("','", "'", "'"));
                    this.updatePermissions(roleName, resourceName, grantModeColumn, "-", perms);
                    revokedPermissions.addAll(existingPermissions);
                }
            }

            return revokedPermissions;
        }
    }

    private void updatePermissions(String roleName, String resourceName, String grantModeColumn, String op, String perms) {
        process("UPDATE system_auth.role_permissions SET %s = %s %s { %s } WHERE role = '%s' AND resource = '%s'", new Object[]{grantModeColumn, grantModeColumn, op, perms, roleName, resourceName});
    }

    private Set<Permission> getExistingPermissions(String roleName, String resourceName, String grantModeColumn, Set<Permission> expectedPermissions) {
        UntypedResultSet rs = process("SELECT %s FROM system_auth.role_permissions WHERE role = '%s' AND resource = '%s'", new Object[]{grantModeColumn, roleName, resourceName});
        if (rs.isEmpty()) {
            return Collections.emptySet();
        } else {
            UntypedResultSet.Row one = rs.one();
            if (!one.has(grantModeColumn)) {
                return Collections.emptySet();
            } else {
                Set<Permission> existingPermissions = Sets.newHashSetWithExpectedSize(expectedPermissions.size());
                Iterator var8 = one.getSet(grantModeColumn, UTF8Type.instance).iterator();

                while (var8.hasNext()) {
                    String permissionName = (String) var8.next();
                    Permission permission = Permissions.permission(permissionName);
                    if (expectedPermissions.contains(permission)) {
                        existingPermissions.add(permission);
                    }
                }

                return existingPermissions;
            }
        }
    }

    public Set<PermissionDetails> list(Set<Permission> permissions, IResource resource, RoleResource grantee) {
        Set<RoleResource> roles = grantee != null ? DatabaseDescriptor.getRoleManager().getRolesIncludingGrantee(grantee, true) : Collections.emptySet();
        CassandraAuthorizer.VersionDepentent impl = (CassandraAuthorizer.VersionDepentent) this.feature.implementation();
        Set<PermissionDetails> details = new HashSet();
        Iterator var7 = process("%s", new Object[]{impl.buildListQuery(resource, roles)}).iterator();

        while (var7.hasNext()) {
            UntypedResultSet.Row row = (UntypedResultSet.Row) var7.next();
            PermissionSets.Builder permsBuilder = PermissionSets.builder();
            impl.addPermissionsFromRow(row, permsBuilder);
            PermissionSets perms = permsBuilder.build();
            String rowRole = row.getString("role");
            IResource rowResource = Resources.fromName(row.getString("resource"));
            Iterator var13 = perms.allContainedPermissions().iterator();

            while (var13.hasNext()) {
                Permission p = (Permission) var13.next();
                if (permissions.contains(p)) {
                    details.add(new PermissionDetails(rowRole, rowResource, p, perms.grantModesFor(p)));
                }
            }
        }

        return details;
    }

    public Set<DataResource> protectedResources() {
        return ImmutableSet.of(DataResource.table("system_auth", "role_permissions"));
    }

    public void validateConfiguration() throws ConfigurationException {
    }

    public void setup() {
        this.feature.setup(Gossiper.instance.clusterVersionBarrier);
    }

    private static String escape(String name) {
        return StringUtils.replace(name, "'", "''");
    }

    private static UntypedResultSet process(String query, Object... arguments) throws RequestExecutionException {
        String cql = String.format(query, arguments);
        return QueryProcessor.processBlocking(cql, ConsistencyLevel.LOCAL_ONE);
    }

    private static final class Native extends CassandraAuthorizer.VersionDepentent {
        private static final String listQuery = "SELECT role, resource, permissions, restricted, grantables FROM system_auth.role_permissions";
        private static final String rolesQuery = "SELECT role, resource, permissions, restricted, grantables FROM system_auth.role_permissions WHERE role IN ?";
        private SelectStatement permissionsForRolesStatement;

        Native() {
            super("SELECT role, resource, permissions, restricted, grantables FROM system_auth.role_permissions");
        }

        void addPermissionsFromRow(UntypedResultSet.Row row, PermissionSets.Builder perms) {
            CassandraAuthorizer.permissionsFromRow(row, "permissions", perms::addGranted);
            CassandraAuthorizer.permissionsFromRow(row, "restricted", perms::addRestricted);
            CassandraAuthorizer.permissionsFromRow(row, "grantables", perms::addGrantable);
        }

        String columnForGrantMode(GrantMode grantMode) {
            switch (grantMode) {
                case GRANT: {
                    return CassandraAuthorizer.PERMISSIONS;
                }
                case RESTRICT: {
                    return CassandraAuthorizer.RESTRICTED;
                }
                case GRANTABLE: {
                    return CassandraAuthorizer.GRANTABLES;
                }
            }
            throw new AssertionError();
        }

        Map<IResource, PermissionSets> permissionsForRole(RoleResource role) {
            return (Map) this.permissionsForRoles(Collections.singleton(role)).get(role);
        }

        Map<RoleResource, Map<IResource, PermissionSets>> permissionsForRoles(Iterable<? extends RoleResource> roles) {
            Map<RoleResource, Map<IResource, PermissionSets>> roleResourcePermissions = new HashMap();
            List<String> names = new ArrayList();
            Iterator var4 = roles.iterator();

            while (var4.hasNext()) {
                RoleResource role = (RoleResource) var4.next();
                names.add(role.getRoleName());
                roleResourcePermissions.put(role, new HashMap());
            }

            ByteBuffer roleNames = ListSerializer.getInstance(UTF8Serializer.instance).serialize(names);
            QueryOptions options = QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE, Collections.singletonList(roleNames));
            SelectStatement st = this.permissionsForRolesStatement;
            ResultMessage.Rows rows = (ResultMessage.Rows) TPCUtils.blockingGet(st.execute(QueryState.forInternalCalls(), options, System.nanoTime()));
            UntypedResultSet result = UntypedResultSet.create(rows.result);
            if (result.isEmpty()) {
                return roleResourcePermissions;
            } else {
                Iterator var9 = result.iterator();

                while (var9.hasNext()) {
                    UntypedResultSet.Row row = (UntypedResultSet.Row) var9.next();
                    IResource resource = Resources.fromName(row.getString("resource"));
                    PermissionSets.Builder builder = PermissionSets.builder();
                    this.addPermissionsFromRow(row, builder);
                    RoleResource role = RoleResource.role(row.getString("role"));
                    Map<IResource, PermissionSets> resourcePermissions = (Map) roleResourcePermissions.get(role);
                    resourcePermissions.put(resource, builder.build());
                }

                return roleResourcePermissions;
            }
        }

        public void initialize() {
            this.permissionsForRolesStatement = (SelectStatement) QueryProcessor.getStatement("SELECT role, resource, permissions, restricted, grantables FROM system_auth.role_permissions WHERE role IN ?", QueryState.forInternalCalls()).statement;
        }
    }

    private static final class Legacy extends CassandraAuthorizer.VersionDepentent {
        private static final String listQuery = "SELECT role, resource, permissions FROM system_auth.role_permissions";
        private static final String rolesQuery = "SELECT role, resource, permissions FROM system_auth.role_permissions WHERE role IN ?";
        private SelectStatement permissionsForRolesStatement;

        Legacy() {
            super("SELECT role, resource, permissions FROM system_auth.role_permissions");
        }

        void addPermissionsFromRow(UntypedResultSet.Row row, PermissionSets.Builder perms) {
            CassandraAuthorizer.permissionsFromRow(row, "permissions", perms::addGranted);
        }

        String columnForGrantMode(GrantMode grantMode) {
            switch (grantMode) {
                case GRANT: {
                    return CassandraAuthorizer.PERMISSIONS;
                }
                case RESTRICT:
                case GRANTABLE: {
                    throw new InvalidRequestException("GRANT AUTHORIZE FOR + RESTRICT are not available until all nodes are on DSE 6.0");
                }
            }
            throw new AssertionError();
        }

        Map<IResource, PermissionSets> permissionsForRole(RoleResource role) {
            return (Map) this.permissionsForRoles(Collections.singleton(role)).get(role);
        }

        Map<RoleResource, Map<IResource, PermissionSets>> permissionsForRoles(Iterable<? extends RoleResource> roles) {
            HashMap<RoleResource, Map<IResource, PermissionSets>> roleResourcePermissions = new HashMap<RoleResource, Map<IResource, PermissionSets>>();
            ArrayList<String> names = new ArrayList<String>();
            for (RoleResource role : roles) {
                names.add(role.getRoleName());
                roleResourcePermissions.put(role, new HashMap());
            }
            ByteBuffer roleNames = ListSerializer.getInstance(UTF8Serializer.instance).serialize(names);
            QueryOptions options = QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE, Collections.singletonList(roleNames));
            SelectStatement st = this.permissionsForRolesStatement;
            ResultMessage.Rows rows = TPCUtils.blockingGet(st.execute(QueryState.forInternalCalls(), options, System.nanoTime()));
            UntypedResultSet result = UntypedResultSet.create(rows.result);
            if (result.isEmpty()) {
                return roleResourcePermissions;
            }
            for (UntypedResultSet.Row row : result) {
                IResource resource = Resources.fromName(row.getString(CassandraAuthorizer.RESOURCE));
                PermissionSets.Builder builder = PermissionSets.builder();
                this.addPermissionsFromRow(row, builder);
                RoleResource role = RoleResource.role(row.getString(CassandraAuthorizer.ROLE));
                Map<IResource, PermissionSets> resourcePermissions = roleResourcePermissions.get(role);
                resourcePermissions.put(resource, builder.build());
            }
            return roleResourcePermissions;
        }

        public void initialize() {
            this.permissionsForRolesStatement = (SelectStatement) QueryProcessor.getStatement("SELECT role, resource, permissions FROM system_auth.role_permissions WHERE role IN ?", QueryState.forInternalCalls()).statement;
        }
    }

    private abstract static class VersionDepentent implements VersionDependentFeature.VersionDependent {
        private final String listQuery;

        VersionDepentent(String listQuery) {
            this.listQuery = listQuery;
        }

        String buildListQuery(IResource resource, Set<RoleResource> roles) {
            StringBuilder builder = new StringBuilder(this.listQuery);
            boolean hasResource = resource != null;
            boolean hasRoles = roles != null && !roles.isEmpty();
            if (hasResource) {
                builder.append(" WHERE resource = '").append(CassandraAuthorizer.escape(resource.getName())).append('\'');
            }

            if (hasRoles) {
                builder.append(hasResource ? " AND " : " WHERE ").append("role IN ").append((String) roles.stream().map((r) -> {
                    return CassandraAuthorizer.escape(r.getRoleName());
                }).collect(Collectors.joining("', '", "('", "')")));
            }

            builder.append(" ALLOW FILTERING");
            return builder.toString();
        }

        abstract void addPermissionsFromRow(UntypedResultSet.Row var1, PermissionSets.Builder var2);

        abstract String columnForGrantMode(GrantMode var1);

        abstract Map<IResource, PermissionSets> permissionsForRole(RoleResource var1);

        abstract Map<RoleResource, Map<IResource, PermissionSets>> permissionsForRoles(Iterable<? extends RoleResource> var1);
    }
}
