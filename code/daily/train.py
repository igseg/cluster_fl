import pandas as pd
import numpy  as np
from time import time
from generalized_tools import (COS_method_call_general,
                                path_generating,
                                setting_generating,
                                x0_generating,
                                _calc_cumulants,
                                generate_params_cos_1)
from tools import get_best_estimates, implied_volatility
from scipy.optimize import minimize
import sys
import os

def _extract_arrays(df):
    return (
        df['ttm'].to_numpy(),
        df['S0'].to_numpy(),
        df['k'].to_numpy(),
        df['mid_price'].to_numpy(),
        df['bsiv'].to_numpy(),
        df['vega'].to_numpy(),
        df['type'].to_numpy(),
    )

def obj_fun_1(state, r, arrays, loss_f, N, model_name):
    tau_a, S0_a, k_a, c_obs_a, bsiv_obs_a, vega_a, type_a = arrays
    n_obs = len(tau_a)
    objective = 0.0
    cum_cache = {}
    disc_cache = {}
    for j in range(n_obs):
        tau = tau_a[j]
        S0  = S0_a[j]
        k   = k_a[j]
        c_obs = c_obs_a[j]
        bsiv_obs = bsiv_obs_a[j]
        vega = vega_a[j]
        type = type_a[j]

        cumulants = cum_cache.get(tau)
        if cumulants is None:
            cumulants = _calc_cumulants(tau, r, state, [1,2,3,4], model_name)
            cum_cache[tau] = cumulants
        discount = disc_cache.get(tau)
        if discount is None:
            discount = np.exp(-r * tau)
            disc_cache[tau] = discount

        params_cos = generate_params_cos_1(state, r, tau, S0, k, type, model_name)
        try:
            option_price = COS_method_call_general(params_cos, discount=discount, K=k, N=N, L=10, c=cumulants, model_name=model_name)
        except ZeroDivisionError:
            option_price = 0
        if np.isnan(option_price):
            option_price = 0
        if loss_f == 'price':
            loss = np.pow(option_price - c_obs,2)
        if loss_f == 'price_vega':
            loss = np.pow((option_price - c_obs)/vega,2)
        if loss_f == 'iv':
            try:
                bsiv = implied_volatility(option_price, S=S0, K=k, T=tau, r=r, option_type="call")
            except ValueError:
                bsiv = 0
            loss = np.pow(bsiv - bsiv_obs,2)

        loss_real = loss.real
        if loss_real > 1e-6:
            objective += loss_real
    return np.sqrt(objective / n_obs)

def training_1_step(model_name, window, start, batch_size):
    ## Load Data
    file = f'../../Data/trainable_df_{window}.csv'
    df = pd.read_csv(file)
    df = df.assign(estimated_price=np.zeros(len(df)))
    df = df.assign(iv_estimated=np.zeros(len(df)))

    if model_name == 'cgmy_sv_1':
        N = 128
    elif model_name == 'cgmy4_sv_1':
        N = 256
    elif model_name == 'heston_1_step':
        N = 128
    else:
        N = 16

    df.timestamp = pd.to_datetime(df.timestamp) - pd.Timedelta(hours=8)

    df['date'] = df.timestamp.dt.date

    ## Subsample
    # print(sorted(df['T'].unique()[-5:]))
    # df = df[np.isin(df['T'], sorted(df['T'].unique()[-30:))]

    print(df.head())
    ## Parameters
    r=0
    loss_f = 'price_vega'
    tol=1e-3
    t0 = time()
    T = len(df['T'].unique())
    pad = int(np.log10(T)) + 1

    path_estimates, path_parameter = path_generating(model_name)
    os.makedirs(path_estimates, exist_ok=True)
    os.makedirs(path_parameter, exist_ok=True)
    optimizer_setting = setting_generating(model_name)

    ## Define the meshgrid of starting points

    mesh_points = x0_generating(model_name)
    t0 = time()
    # for i in sorted(df['T'].unique())[906:]: ## to skip times
    # for i in sorted(df['T'].unique()):
    end = len(df['T'].unique())
    if start + batch_size >= end:
        batch_size = end - start
    for i in sorted(df['T'].unique())[start:start+batch_size]:
        best_state = np.array([np.nan]*5)
        best_result = np.nan
        first_run = True
        iteration = 0
        tmp_df = df[df['T']==i]
        arrays = _extract_arrays(tmp_df)

        print('='*70)
        for index in np.ndindex(mesh_points.shape[:-1]):
            iteration+=1
            state = mesh_points[index]
            obj_func_1_opt = lambda x: obj_fun_1(x, r, arrays, loss_f, N, model_name)
            # try:
            results_step_1 = minimize(obj_func_1_opt, state, **optimizer_setting)
            print(f'Attempt: {iteration}')
            print(results_step_1.fun)
            print(results_step_1.x)
            print(f'Time: {(time()-t0)/60:.2f}')
            print('--'*20)
            # except ZeroDivisionError:
            #     print('aborted')
            #     continue
            # print(results_step_1.fun)
            if first_run:
                first_run = False
                best_state = results_step_1.x
                best_result = results_step_1.fun
                if best_result < tol:
                        break
            else:
                if results_step_1.fun < best_result:
                    best_state = results_step_1.x
                    best_result = results_step_1.fun
                    if best_result < tol:
                        break

        time_elapsed = time() - t0
        time_per_iter = (time() - t0)/(60 * ((i - start)+1))
        print(best_state)
        print(f'Iteration {i+1} of {T+1}')
        print(f'Loss function: {best_result:.2f}')
        print(f'Time per iteration: {time_per_iter:.2f}m')
        print(f'Time remaining: {(T-i) * (time_per_iter / 60):.2f} hours')

        tmp_df = add_iv_estimated(best_state, tmp_df, r, N, model_name)

        os.makedirs(path_estimates, exist_ok=True)
        os.makedirs(path_parameter, exist_ok=True)

        file_estimates = path_estimates + model_name + str(i).zfill(pad) + '.csv'
        file_parameter = path_parameter + model_name + str(i).zfill(pad) + '.npy'

        np.save(file_parameter, best_state)
        tmp_df.to_csv(file_estimates, index=False)


def add_iv_estimated(best_state, tmp_df, r, N, model_name):
    tau_a, S0_a, k_a, _c, _b, _v, type_a = _extract_arrays(tmp_df)
    n_obs = len(tau_a)
    est_price = np.empty(n_obs)
    est_iv = np.empty(n_obs)
    cum_cache = {}
    disc_cache = {}
    for j in range(n_obs):
        tau = tau_a[j]
        S0  = S0_a[j]
        k   = k_a[j]
        type = type_a[j]

        cumulants = cum_cache.get(tau)
        if cumulants is None:
            cumulants = _calc_cumulants(tau, r, best_state, [1,2,3,4], model_name)
            cum_cache[tau] = cumulants
        discount = disc_cache.get(tau)
        if discount is None:
            discount = np.exp(-r * tau)
            disc_cache[tau] = discount

        params_cos = generate_params_cos_1(best_state, r, tau, S0, k, type, model_name)
        option_price = COS_method_call_general(params_cos, discount=discount, K=k, N=N, L=10, c=cumulants, model_name=model_name)

        est_price[j] = option_price
        est_iv[j] = implied_volatility(option_price, S=S0, K=k, T=tau, r=r, option_type=type)

    tmp_df = tmp_df.copy()
    tmp_df['estimated_price'] = est_price
    tmp_df['iv_estimated'] = est_iv
    return tmp_df



if __name__ == '__main__':

    ## Assign model

    # model_name = 'heston_1_step'
    # model_name = 'gaussian_jumps_1'
    # model_name = 'variance_gamma_1'
    model_name = 'cgmy_1'
    # model_name = 'cgmy_sv_1'
    # model_name = 'cgmy4_sv_1'
    time_window = '1day'

    start = int(sys.argv[1])
    batch_size = int(sys.argv[2])

    print(start)
    print(batch_size)

    ## Train

    if '2' in model_name:
        _ = training_2_step(model_name, time_window)
    if '1' in model_name:
        _ = training_1_step(model_name, time_window, start, batch_size)
