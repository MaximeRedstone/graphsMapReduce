/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lstswap_contents.c                              :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2020/01/03 12:42:37 by sikpenou          #+#    #+#             */
/*   Updated: 2020/01/03 12:44:03 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	ft_lstswap_contents(t_lst *elem_1, t_lst *elem_2)
{
	void	*tmp;

	tmp = elem_1->content;
	elem_1->content = elem_2->content;
	elem_2->content = tmp;
}
