import React from 'react';
import Header from '../../Header';
import Table from '../../../components/Table';
import Root from '../../../components/Root';
import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { withRouter } from '../../../utils/withRouter';
import { uriAdminEmployees, uriAdminEmployeeManual } from '../../../utils/endpoints';

class AdminEmployeeList extends Root {
  state = {
    allEmployees: [],
    data: [],
    search: '',
    loading: true,
    showCreate: false,
    createForm: { employeeCode: '', fullName: '', password: '', admin: false }
  };

  componentDidMount() {
    this.loadEmployees();
  }

  async loadEmployees() {
    this.setState({ loading: true });
    try {
      const res = await this.getApi(uriAdminEmployees());
      this.setState({ allEmployees: res.data || [], loading: false }, () => this.applyFilter());
    } catch (err) {
      this.setState({ loading: false });
    }
  }

  toRow(e) {
    return {
      id: e.employeeCode,
      employeeCode: e.employeeCode,
      fullName: e.fullName,
      role: e.superAdmin ? 'Super Admin' : e.admin ? 'Admin' : 'Employee',
      teams: (e.teams || []).join(', ') || '-',
      accountType: e.accountType,
      status: e.active ? 'Active' : 'Deactivated'
    };
  }

  applyFilter() {
    const { allEmployees, search } = this.state;
    const needle = search.trim().toLowerCase();
    const filtered = !needle
      ? allEmployees
      : allEmployees.filter(
          e =>
            e.employeeCode.toLowerCase().includes(needle) ||
            e.fullName.toLowerCase().includes(needle)
        );
    this.setState({ data: filtered.map(e => this.toRow(e)) });
  }

  handleSearchChange = e => {
    this.setState({ search: e.target.value }, () => this.applyFilter());
  };

  handleCreateChange = e => {
    const { name, value, type, checked } = e.target;
    this.setState({
      createForm: { ...this.state.createForm, [name]: type === 'checkbox' ? checked : value }
    });
  };

  createManualAccount = async e => {
    e.preventDefault();
    const { createForm } = this.state;
    if (!createForm.employeeCode || !createForm.fullName || createForm.password.length < 8) {
      toast.error('Employee code, full name, and a password of at least 8 characters are required');
      return;
    }
    try {
      await this.postApi(uriAdminEmployeeManual(), createForm);
      toast.success('Account created');
      this.setState({
        showCreate: false,
        createForm: { employeeCode: '', fullName: '', password: '', admin: false }
      });
      this.loadEmployees();
    } catch (err) {
      // error already toasted by the api layer
    }
  };

  render() {
    const { data, search, loading, showCreate, createForm } = this.state;

    return (
      <div>
        <Header title="Admin - Employees">
          <button
            className="btn btn-primary ms-2"
            onClick={() => this.setState({ showCreate: !showCreate })}
          >
            {showCreate ? 'Close' : 'Create manual account'}
          </button>
        </Header>

        {showCreate && (
          <form className="khq-data-filter khq-nav p-3 mb-3" onSubmit={this.createManualAccount}>
            <div className="row g-2 align-items-end">
              <div className="col-auto">
                <label className="form-label">Employee code</label>
                <input
                  className="form-control"
                  name="employeeCode"
                  value={createForm.employeeCode}
                  onChange={this.handleCreateChange}
                />
              </div>
              <div className="col-auto">
                <label className="form-label">Full name</label>
                <input
                  className="form-control"
                  name="fullName"
                  value={createForm.fullName}
                  onChange={this.handleCreateChange}
                />
              </div>
              <div className="col-auto">
                <label className="form-label">Password</label>
                <input
                  type="password"
                  className="form-control"
                  name="password"
                  value={createForm.password}
                  onChange={this.handleCreateChange}
                />
              </div>
              <div className="col-auto form-check mb-2">
                <input
                  type="checkbox"
                  className="form-check-input"
                  id="createAdmin"
                  name="admin"
                  checked={createForm.admin}
                  onChange={this.handleCreateChange}
                />
                <label className="form-check-label" htmlFor="createAdmin">
                  Admin
                </label>
              </div>
              <div className="col-auto">
                <button type="submit" className="btn btn-primary">
                  Create
                </button>
              </div>
            </div>
          </form>
        )}

        <nav className="navbar navbar-expand-lg navbar-light bg-light me-auto khq-data-filter khq-sticky khq-nav">
          <input
            type="text"
            className="form-control"
            placeholder="Search by employee code or name"
            value={search}
            onChange={this.handleSearchChange}
          />
        </nav>

        <Table
          loading={loading}
          columns={[
            { id: 'employeeCode', accessor: 'employeeCode', colName: 'Employee code', sortable: true },
            { id: 'fullName', accessor: 'fullName', colName: 'Name', sortable: true },
            { id: 'role', accessor: 'role', colName: 'Role', sortable: true },
            { id: 'teams', accessor: 'teams', colName: 'Teams' },
            { id: 'accountType', accessor: 'accountType', colName: 'Login method' },
            { id: 'status', accessor: 'status', colName: 'Status' }
          ]}
          actions={['details']}
          data={data}
          updateData={updated => this.setState({ data: updated })}
          noContent="No employees yet - they appear here the first time they log in, or you create a manual account above."
          detailsHref={code => `/ui/admin/employees/${encodeURIComponent(code)}`}
        />
      </div>
    );
  }
}

export default withRouter(AdminEmployeeList);
