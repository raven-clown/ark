import React from 'react';
import Header from '../../Header';
import Table from '../../../components/Table';
import Root from '../../../components/Root';
import ConfirmModal from '../../../components/Modal/ConfirmModal';
import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { withRouter } from '../../../utils/withRouter';
import {
  uriAdminEmployee,
  uriAdminEmployeeAdmin,
  uriAdminEmployeeSuperAdmin,
  uriAdminEmployeeActive,
  uriAdminEmployeeTeams,
  uriAdminEmployeeTeam,
  uriAdminEmployeeGrants,
  uriAdminEmployeeGrant,
  uriAdminEmployeePassword,
  uriAdminTeams
} from '../../../utils/endpoints';

const RESOURCES = [
  'TOPIC',
  'TOPIC_DATA',
  'CONSUMER_GROUP',
  'CONNECT_CLUSTER',
  'CONNECTOR',
  'SCHEMA',
  'NODE',
  'ACL',
  'KSQLDB'
];

const ACTIONS = [
  'READ',
  'CREATE',
  'UPDATE',
  'DELETE',
  'UPDATE_OFFSET',
  'DELETE_OFFSET',
  'READ_CONFIG',
  'ALTER_CONFIG',
  'DELETE_VERSION',
  'UPDATE_STATE',
  'EXECUTE'
];

class AdminEmployeeDetail extends Root {
  state = {
    employee: null,
    allTeams: [],
    teamToAdd: '',
    grantForm: { resource: 'TOPIC', action: 'READ', clusterPattern: '.*', topicPattern: '.*' },
    newPassword: '',
    loading: true,
    confirm: null
  };

  componentDidMount() {
    this.load();
    this.getApi(uriAdminTeams()).then(res => this.setState({ allTeams: res.data || [] }));
  }

  employeeCode() {
    return this.props.params.employeeCode;
  }

  async load() {
    this.setState({ loading: true });
    try {
      const res = await this.getApi(uriAdminEmployee(this.employeeCode()));
      this.setState({ employee: res.data, loading: false });
    } catch (err) {
      this.setState({ loading: false });
    }
  }

  toggleAdmin = () => {
    const { employee } = this.state;
    this.patchApi(uriAdminEmployeeAdmin(employee.employeeCode), { admin: !employee.admin })
      .then(() => {
        toast.success('Updated');
        this.load();
      })
      .catch(() => {});
  };

  toggleSuperAdmin = () => {
    const { employee } = this.state;
    this.patchApi(uriAdminEmployeeSuperAdmin(employee.employeeCode), {
      admin: !employee.superAdmin
    })
      .then(() => {
        toast.success('Updated');
        this.load();
      })
      .catch(() => {});
  };

  toggleActive = () => {
    const { employee } = this.state;
    this.patchApi(uriAdminEmployeeActive(employee.employeeCode), { active: !employee.active })
      .then(() => {
        toast.success('Updated');
        this.load();
      })
      .catch(() => {});
  };

  addTeam = () => {
    const { teamToAdd, employee } = this.state;
    if (!teamToAdd) return;
    this.postApi(uriAdminEmployeeTeams(employee.employeeCode), { teamName: teamToAdd })
      .then(() => {
        toast.success('Team assigned');
        this.setState({ teamToAdd: '' });
        this.load();
      })
      .catch(() => {});
  };

  removeTeam = teamName => {
    const { employee } = this.state;
    this.removeApi(uriAdminEmployeeTeam(employee.employeeCode, teamName))
      .then(() => {
        toast.success('Team removed');
        this.load();
      })
      .catch(() => {});
  };

  handleGrantFormChange = e => {
    const { name, value } = e.target;
    this.setState({ grantForm: { ...this.state.grantForm, [name]: value } });
  };

  addGrant = e => {
    e.preventDefault();
    const { employee, grantForm } = this.state;
    this.postApi(uriAdminEmployeeGrants(employee.employeeCode), grantForm)
      .then(() => {
        toast.success('Permission granted');
        this.load();
      })
      .catch(() => {});
  };

  revokeGrant = grant => {
    const { employee } = this.state;
    if (grant.source !== 'INDIVIDUAL') {
      toast.warn('Team-level grants are managed from the team, not from this employee');
      return;
    }
    this.removeApi(uriAdminEmployeeGrant(employee.employeeCode, grant.id))
      .then(() => {
        toast.success('Permission revoked');
        this.load();
      })
      .catch(() => {});
  };

  resetPassword = e => {
    e.preventDefault();
    const { employee, newPassword } = this.state;
    if (newPassword.length < 8) {
      toast.error('Password must be at least 8 characters');
      return;
    }
    this.patchApi(uriAdminEmployeePassword(employee.employeeCode), { password: newPassword })
      .then(() => {
        toast.success('Password reset');
        this.setState({ newPassword: '' });
      })
      .catch(() => {});
  };

  render() {
    const { employee, allTeams, teamToAdd, grantForm, newPassword, loading, confirm } = this.state;

    if (loading || !employee) {
      return (
        <div>
          <Header title="Admin - Employee" />
        </div>
      );
    }

    const grantRows = (employee.grants || []).map((g, i) => ({
      id: g.id || i,
      source: g.source,
      resource: g.resource,
      action: g.action,
      clusterPattern: g.clusterPattern,
      topicPattern: g.topicPattern,
      raw: g
    }));

    const assignableTeams = allTeams.filter(t => !(employee.teams || []).includes(t.name));

    return (
      <div>
        <Header title={`Admin - ${employee.fullName} (${employee.employeeCode})`} />

        <div className="khq-data-filter khq-nav p-3 mb-3">
          <div className="row">
            <div className="col-md-6">
              <p>
                <b>Login method:</b> {employee.accountType}
              </p>
              <p>
                <b>Last login:</b> {employee.lastLoginAt || 'never'}
              </p>
              <p>
                <b>Status:</b> {employee.active ? 'Active' : 'Deactivated'}
              </p>
            </div>
            <div className="col-md-6 text-md-end">
              <button className="btn btn-secondary me-2 mb-2" onClick={this.toggleAdmin}>
                {employee.admin ? 'Revoke admin' : 'Grant admin'}
              </button>
              <button className="btn btn-secondary me-2 mb-2" onClick={this.toggleSuperAdmin}>
                {employee.superAdmin ? 'Revoke super admin' : 'Grant super admin'}
              </button>
              <button
                className="btn btn-danger mb-2"
                onClick={() =>
                  this.setState({
                    confirm: {
                      message: employee.active
                        ? `Deactivate ${employee.employeeCode}? They will not be able to log in.`
                        : `Reactivate ${employee.employeeCode}?`,
                      onConfirm: () => {
                        this.setState({ confirm: null });
                        this.toggleActive();
                      }
                    }
                  })
                }
              >
                {employee.active ? 'Deactivate' : 'Reactivate'}
              </button>
            </div>
          </div>
        </div>

        <h3>Teams</h3>
        <div className="khq-data-filter khq-nav p-3 mb-3">
          {(employee.teams || []).length === 0 && <p className="mb-2">No team assigned.</p>}
          {(employee.teams || []).map(t => (
            <span key={t} className="badge bg-primary me-2 mb-2">
              {t}{' '}
              <span style={{ cursor: 'pointer' }} onClick={() => this.removeTeam(t)}>
                ×
              </span>
            </span>
          ))}
          <div className="row g-2 mt-2 align-items-end">
            <div className="col-auto">
              <select
                className="form-select"
                value={teamToAdd}
                onChange={e => this.setState({ teamToAdd: e.target.value })}
              >
                <option value="">Assign a team...</option>
                {assignableTeams.map(t => (
                  <option key={t.name} value={t.name}>
                    {t.name}
                  </option>
                ))}
              </select>
            </div>
            <div className="col-auto">
              <button className="btn btn-primary" onClick={this.addTeam}>
                Assign
              </button>
            </div>
          </div>
        </div>

        <h3>Permissions</h3>
        <Table
          loading={false}
          columns={[
            { id: 'source', accessor: 'source', colName: 'Source' },
            { id: 'resource', accessor: 'resource', colName: 'Resource' },
            { id: 'action', accessor: 'action', colName: 'Action' },
            { id: 'clusterPattern', accessor: 'clusterPattern', colName: 'Cluster pattern' },
            { id: 'topicPattern', accessor: 'topicPattern', colName: 'Topic/resource pattern' }
          ]}
          actions={['delete']}
          data={grantRows}
          updateData={() => {}}
          onDelete={row => this.revokeGrant(row.raw)}
          noContent="No permissions granted yet."
        />

        <form className="khq-data-filter khq-nav p-3 mb-3 mt-3" onSubmit={this.addGrant}>
          <div className="row g-2 align-items-end">
            <div className="col-auto">
              <label className="form-label">Resource</label>
              <select
                className="form-select"
                name="resource"
                value={grantForm.resource}
                onChange={this.handleGrantFormChange}
              >
                {RESOURCES.map(r => (
                  <option key={r} value={r}>
                    {r}
                  </option>
                ))}
              </select>
            </div>
            <div className="col-auto">
              <label className="form-label">Action</label>
              <select
                className="form-select"
                name="action"
                value={grantForm.action}
                onChange={this.handleGrantFormChange}
              >
                {ACTIONS.map(a => (
                  <option key={a} value={a}>
                    {a}
                  </option>
                ))}
              </select>
            </div>
            <div className="col-auto">
              <label className="form-label">Cluster pattern</label>
              <input
                className="form-control"
                name="clusterPattern"
                value={grantForm.clusterPattern}
                onChange={this.handleGrantFormChange}
              />
            </div>
            <div className="col-auto">
              <label className="form-label">Topic/resource pattern</label>
              <input
                className="form-control"
                name="topicPattern"
                value={grantForm.topicPattern}
                onChange={this.handleGrantFormChange}
              />
            </div>
            <div className="col-auto">
              <button type="submit" className="btn btn-primary">
                Grant permission
              </button>
            </div>
          </div>
        </form>

        {employee.accountType === 'MANUAL' && (
          <>
            <h3>Reset password</h3>
            <form className="khq-data-filter khq-nav p-3 mb-3" onSubmit={this.resetPassword}>
              <div className="row g-2 align-items-end">
                <div className="col-auto">
                  <input
                    type="password"
                    className="form-control"
                    placeholder="New password"
                    value={newPassword}
                    onChange={e => this.setState({ newPassword: e.target.value })}
                  />
                </div>
                <div className="col-auto">
                  <button type="submit" className="btn btn-primary">
                    Reset password
                  </button>
                </div>
              </div>
            </form>
          </>
        )}

        <ConfirmModal
          show={!!confirm}
          message={confirm ? confirm.message : ''}
          handleConfirm={() => confirm && confirm.onConfirm()}
          handleCancel={() => this.setState({ confirm: null })}
        />
      </div>
    );
  }
}

export default withRouter(AdminEmployeeDetail);
