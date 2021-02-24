/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_find_next_prime.c                               :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/02/07 09:37:21 by sikpenou          #+#    #+#             */
/*   Updated: 2019/10/30 23:05:03 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

int		ft_is_prime(int nb)
{
	long int	divisor;

	if (nb == 2)
		return (1);
	if (nb < 2 || nb % 2 == 0)
		return (0);
	divisor = 3;
	while (divisor * divisor <= nb)
	{
		if (nb % divisor == 0)
			return (0);
		divisor += 2;
	}
	return (1);
}

int		ft_find_next_prime(int nb)
{
	if (nb == 2)
		return (2);
	if (!(nb % 2))
	{
		if (ft_is_prime(nb))
			return (nb);
		++nb;
	}
	while (ft_is_prime(nb) == 0)
		nb += 2;
	return (nb);
}
